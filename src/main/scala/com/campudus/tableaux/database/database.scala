package com.campudus.tableaux.database

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.helper.VertxAccess

import io.vertx.core.Vertx
import io.vertx.core.json.{Json => VertxJson}
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.lang.scala.json.{JsonArray, JsonObject}
import io.vertx.scala.{DatabaseAction, SQLConnection}
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet
import io.vertx.sqlclient.data.Numeric

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

trait DatabaseQuery extends LazyLogging {
  protected val connection: DatabaseConnection

  implicit val executionContext: VertxExecutionContext = connection.executionContext

  protected def checkUpdateResults(seq: JsonObject*): Unit = {
    seq.map(json => if (json.containsKey("message")) updateNotNull(json))
  }

  protected def optionToValidFuture[A, B](
      opt: Option[A],
      trans: B,
      someCase: A => Future[(B, JsonObject)]
  ): Future[(B, JsonObject)] = {
    opt match {
      case Some(x) => someCase(x)
      case None => Future.successful(trans, Json.obj())
    }
  }

  protected def convertStringToDateTime(str: String): Option[DateTime] = {
    Option(str).map(DateTime.parse)
  }

  protected def convertJsonArrayToSeq[A](arr: JsonArray, converter: AnyRef => A): Seq[A] = {
    Option(arr).getOrElse(Json.arr()).asScala.toSeq.map(converter)
  }
}

trait DbTransaction {

  def query(stmt: String): Future[(DbTransaction, JsonObject)]

  def query(
      stmt: String,
      values: JsonArray
  ): Future[(DbTransaction, JsonObject)]

  def selectSingleValue[A](
      select: String,
      parameter: JsonArray
  ): Future[(DbTransaction, A)]

  def commit(): Future[Unit]
  def rollbackAndFail(): PartialFunction[Throwable, Future[(DbTransaction, JsonObject)]]
  def rollback(): Future[Unit]
}

object DatabaseConnection {
  type ScalaTransaction = io.vertx.scala.Transaction

  def apply(vertxAccess: VertxAccess, connection: SQLConnection): DatabaseConnection = {
    new DatabaseConnection(vertxAccess, connection)
  }
}

class DatabaseConnection(val vertxAccess: VertxAccess, val connection: SQLConnection) extends VertxAccess {

  import DatabaseConnection._

  override val vertx: Vertx = vertxAccess.vertx

  /**
    * Resets the connection pool by closing all existing connections and creating a new pool. This clears any cached
    * prepared statements that may have become stale after schema changes.
    */
  def resetPool(): Future[Unit] = connection.resetPool()

  type TransFunc[+A] = DbTransaction => Future[(DbTransaction, A)]

  case class Transaction(transaction: ScalaTransaction) extends DbTransaction {

    def query(stmt: String): Future[(DbTransaction, JsonObject)] = {
      doMagicQuery(stmt, None, transaction)
        .map(result => (copy(transaction), result))
        .recoverWith(rollbackAndFail())
    }

    def query(stmt: String, values: JsonArray): Future[(DbTransaction, JsonObject)] = {
      doMagicQuery(stmt, Some(values), transaction)
        .map(result => (copy(transaction), result))
        .recoverWith(rollbackAndFail())
    }

    def selectSingleValue[A](select: String): Future[(DbTransaction, A)] = selectSingleValue(select, None)

    def selectSingleValue[A](select: String, parameter: JsonArray): Future[(DbTransaction, A)] =
      selectSingleValue(select, Some(parameter))

    private def selectSingleValue[A](select: String, parameter: Option[JsonArray]): Future[(DbTransaction, A)] = {
      for {
        (t, resultJson) <- parameter match {
          case None => query(select)
          case Some(p) => query(select, p)
        }
      } yield {
        (t, selectNotNull(resultJson).head.getValue(0).asInstanceOf[A])
      }
    }

    def commit(): Future[Unit] = transaction.commit()

    def rollback(): Future[Unit] = transaction.rollback()

    def rollbackAndFail(): PartialFunction[Throwable, Future[(DbTransaction, JsonObject)]] = {
      case ex: Throwable =>
        logger.error(s"Rollback and fail.", ex)
        rollback() flatMap (_ => Future.failed[(DbTransaction, JsonObject)](ex))
    }
  }

  def query(stmt: String): Future[JsonObject] = doMagicQuery(stmt, None, connection)

  def query(stmt: String, parameter: JsonArray): Future[JsonObject] = doMagicQuery(stmt, Some(parameter), connection)

  def begin(): Future[DbTransaction] = connection.transaction().map(Transaction.apply)

  def transactional[A](fn: TransFunc[A]): Future[A] = {
    for {
      transaction <- begin()

      (transaction, result) <- {
        fn(transaction) recoverWith {
          case e: Throwable =>
            logger.error("Failed executing transactional. Rollback and fail.", e)
            transaction.rollback()
            Future.failed(e)
        }
      }

      _ <- {
        transaction.commit()
      }
    } yield {
      result
    }
  }

  def transactionalFoldLeft[A](values: Seq[A])(
      fn: (DbTransaction, JsonObject, A) => Future[(DbTransaction, JsonObject)]
  ): Future[JsonObject] = {
    transactionalFoldLeft(values, Json.obj())(fn)
  }

  def transactionalFoldLeft[A, B](values: Seq[A], fnStartValue: B)(
      fn: (DbTransaction, B, A) => Future[(DbTransaction, B)]
  ): Future[B] = {
    transactional[B]({ (transaction: DbTransaction) =>
      {
        values.foldLeft(Future(transaction, fnStartValue)) { (result, value) =>
          {
            result.flatMap {
              case (newTransaction, lastResult) =>
                fn(newTransaction, lastResult, value)
            }
          }
        }
      }
    })
  }

  def selectSingleValue[A](select: String): Future[A] = selectSingleValue(select, None)

  def selectSingleValue[A](select: String, parameter: JsonArray): Future[A] =
    selectSingleValue(select, Some(parameter))

  private def selectSingleValue[A](select: String, parameter: Option[JsonArray]): Future[A] = {
    for {
      resultJson <- parameter match {
        case None => query(select)
        case Some(p) => query(select, p)
      }
    } yield {
      selectNotNull(resultJson).head.getValue(0).asInstanceOf[A]
    }
  }

  private def doMagicQuery(stmt: String, values: Option[JsonArray], connection: DatabaseAction): Future[JsonObject] = {
    val command = stmt.trim().split("\\s+").head.toUpperCase
    val returning = stmt.trim().toUpperCase.contains("RETURNING")

    (command, returning) match {
      case ("CREATE", _) | ("DROP", _) | ("ALTER", _) | ("LOCK", _) =>
        connection.execute(stmt).map(_ => createExecuteResult(command))

      case ("UPDATE", true) | ("INSERT", true) | ("SELECT", _) =>
        val future = values match {
          case Some(s) => connection.query(stmt, s)
          case None => connection.query(stmt)
        }
        // Kept as "SELECT" for every command to match the historical message shape of the old client, which
        // ResultChecker.selectNotNull/etc. never actually depend on for UPDATE/INSERT ... RETURNING.
        future.map(mapResultSet)

      case ("DELETE", true) | ("DELETE", false) | ("INSERT", false) | ("UPDATE", false) =>
        val future = values match {
          case Some(s) => connection.update(stmt, s)
          case None => connection.update(stmt)
        }
        future.map(rowSet => mapUpdateResult(command, rowSet))

      case (_, _) =>
        throw DatabaseException(
          s"Command $command in Statement $stmt not supported",
          "error.database.command_not_supported"
        )
    }
  }

  private def createExecuteResult(msg: String): JsonObject = {
    Json.obj(
      "status" -> "ok",
      "message" -> msg,
      "rows" -> 0
    )
  }

  /**
    * The reactive Postgres client never auto-populates generated keys the way the old JDBC-style client did (there's no
    * equivalent of `getGeneratedKeys()`) - callers that need the generated id back use `RETURNING` explicitly and go
    * through `mapResultSet` instead. No caller in this codebase ever relied on the old `keys`/`no_name` shape, so plain
    * (non-RETURNING) statements simply report the affected row count.
    */
  private def mapUpdateResult(command: String, rowSet: RowSet[Row]): JsonObject = {
    val updated = rowSet.rowCount()

    Json.obj(
      "status" -> "ok",
      "rows" -> updated,
      "message" -> s"$command $updated",
      "fields" -> Json.arr(),
      "results" -> Json.arr()
    )
  }

  private def mapResultSet(rowSet: RowSet[Row]): JsonObject = {
    val columnNames = rowSet.columnsNames().asScala.toSeq
    val columnTypes = rowSet.columnDescriptors().asScala.toSeq.map(_.typeName())
    val results = Json.arr(rowSet.iterator().asScala.map(rowToJsonArray(_, columnTypes)).toSeq*)

    Json.obj(
      "status" -> "ok",
      "rows" -> results.size(),
      "message" -> s"SELECT ${results.size()}",
      "fields" -> Json.arr(columnNames*),
      "results" -> results
    )
  }

  private def rowToJsonArray(row: Row, columnTypes: Seq[String]): JsonArray = {
    Json.arr(columnTypes.zipWithIndex.map({
      case (columnType, pos) => normalizeValue(row.getValue(pos), isJsonColumn(columnType))
    })*)
  }

  private def isJsonColumn(columnType: String): Boolean = columnType == "JSON" || columnType == "JSONB"

  /**
    * The reactive Postgres client exposes some column types (NUMERIC, UUID, date/time) as Java types that
    * io.vertx.core.json.JsonObject/JsonArray can't encode directly. Normalize them to the same String/Number shapes the
    * old JDBC-style client produced.
    *
    * jsonb/json columns are handled separately from everything below: the reactive client auto-decodes them into
    * whatever Java type matches the JSON shape - JsonObject/JsonArray for a structure, but a bare Boolean/Number/String
    * (or null) for a JSON scalar. That bare scalar is indistinguishable, by Java type alone, from a real BOOLEAN/
    * NUMERIC/text column (see isJsonColumn/columnDescriptors() above), so it has to be re-stringified based on the
    * column's actual Postgres type, not the decoded value's runtime type. Every call site in this codebase expects the
    * old client's behaviour instead: the raw JSON text as a String, parsed explicitly via Json.obj/arr where needed.
    */
  private def normalizeValue(value: AnyRef, isJsonColumn: Boolean): AnyRef = value match {
    case null => null
    case v if isJsonColumn => VertxJson.encode(v)
    // io.vertx.core.json.JsonObject/JsonArray explicitly reject raw BigDecimal (see JsonObject.checkAndCopy), so
    // NUMERIC columns need to come through as a Long or Double instead, same as the old JDBC-style client did.
    case n: Numeric =>
      val d = n.doubleValue()
      if (!d.isInfinite && d == Math.rint(d) && Math.abs(d) < Long.MaxValue.toDouble) {
        Long.box(d.toLong)
      } else {
        Double.box(d)
      }
    // Same reasoning as Numeric above: JsonObject only understands java.time.Instant natively, not the other
    // java.time types the reactive client returns for TIMESTAMP/DATE/TIME columns. The app already expects
    // timestamp-ish columns to arrive as parseable strings (see DatabaseQuery.convertStringToDateTime), so a plain
    // ISO-8601 String matches the old JDBC-style client's behaviour.
    case t: java.time.LocalDateTime => t.toString
    case t: java.time.OffsetDateTime => t.toString
    case t: java.time.LocalDate => t.toString
    case t: java.time.LocalTime => t.toString
    // uuid columns come back as java.util.UUID; the old client always represented them as plain strings.
    case u: java.util.UUID => u.toString
    // text[]/other array columns come back as a plain Java array; JsonObject only understands JsonArray.
    case arr: Array[AnyRef @unchecked] => Json.arr(arr.map(normalizeValue(_, isJsonColumn = false))*)
    case other => other
  }
}
