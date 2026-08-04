package io.vertx.scala

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.VertxAccess

import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.lang.scala.*
import io.vertx.lang.scala.ImplicitConversions.vertxFutureVoidToScalaFutureUnit
import io.vertx.lang.scala.json.JsonArray
import io.vertx.lang.scala.json.JsonObject
import io.vertx.pgclient.{PgConnection, PgConnectOptions, PgPool}
import io.vertx.sqlclient.{Pool, PoolOptions, Row, RowSet, SqlClient, SqlConnection => JSqlConnection, Tuple}

import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

sealed trait DatabaseAction extends VertxAccess {

  def execute(sql: String): Future[Unit]

  protected def execute(client: SqlClient, sql: String): Future[Unit] =
    SQLConnection.runQuery(client, sql, None).map(_ => ())

  def query(sql: String): Future[RowSet[Row]]

  def query(sql: String, params: JsonArray): Future[RowSet[Row]]

  protected def query(client: SqlClient, sql: String, params: Option[JsonArray]): Future[RowSet[Row]] =
    SQLConnection.runQuery(client, sql, params)

  def update(sql: String): Future[RowSet[Row]]

  def update(sql: String, params: JsonArray): Future[RowSet[Row]]

  protected def update(client: SqlClient, sql: String, params: Option[JsonArray]): Future[RowSet[Row]] =
    SQLConnection.runQuery(client, sql, params)
}

object SQLConnection extends LazyLogging {

  // Postgres severities below WARNING (e.g. the NOTICE emitted by DROP ... CASCADE during a schema reset) are
  // routine and not worth WARNING-level log noise; vertx-pg-client's default notice handler logs everything as
  // a warning (see PgNotice.log), so we install our own to log by actual severity instead.
  private val warnSeverities = Set("WARNING", "ERROR", "FATAL", "PANIC")

  def apply(vertxAccess: VertxAccess, config: JsonObject): SQLConnection = {
    new SQLConnection(vertxAccess, config)
  }

  private def connectOptions(config: JsonObject): PgConnectOptions = {
    val options = new PgConnectOptions()
      .setHost(config.getString("host"))
      .setPort(config.getInteger("port", 5432))
      .setDatabase(config.getString("database"))
      .setUser(config.getString("username"))
      .setCachePreparedStatements(config.getBoolean("cachePreparedStatements", true))

    Option(config.getString("password")).foreach(options.setPassword)

    options
  }

  private def pool(vertx: Vertx, config: JsonObject): Pool = {

    val poolOptions = new PoolOptions().setMaxSize(config.getInteger("maxPoolSize", 10))

    PgPool.pool(vertx, connectOptions(config), poolOptions).connectHandler({
      case conn: PgConnection =>
        conn.noticeHandler(notice => {
          val message =
            s"Backend notice: severity='${notice.getSeverity}', code='${notice.getCode}', message='${notice.getMessage}'"

          if (warnSeverities.contains(notice.getSeverity)) {
            logger.warn(message)
          } else {
            logger.debug(message)
          }
        })
        // connectHandler must close() the connection itself to release it back to the pool
        // (see Pool#connectHandler javadoc) - otherwise every new pooled connection hangs forever.
        conn.close()
      case conn =>
        conn.close()
    })
  }

  /**
    * The old JDBC-style SQL client used `?` as a positional placeholder; the reactive Postgres client requires native
    * `$1, $2, ...` placeholders. Translating here keeps every call site's SQL string unchanged.
    */
  private[scala] def toPositional(sql: String): String = {
    val builder = new StringBuilder(sql.length + 8)
    sql.foldLeft(0) {
      case (index, '?') =>
        val next = index + 1
        builder.append('$').append(next)
        next
      case (index, c) =>
        builder.append(c)
        index
    }
    builder.toString()
  }

  private val UuidPattern =
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$".r

  private val IsoDateTimePattern = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*".r
  private val IsoDatePattern = "^\\d{4}-\\d{2}-\\d{2}$".r

  private def isStringArray(arr: JsonArray): Boolean = {
    arr.getList.asScala.forall(_.isInstanceOf[String])
  }

  private def stringArrayOf(arr: JsonArray): Array[String] = {
    arr.getList.asScala.map(_.asInstanceOf[String]).toArray
  }

  /**
    * The old JDBC-style client happily bound plain Strings/JsonArrays against uuid, timestamptz and text[] columns and
    * let Postgres cast them; the reactive client requires the bound Java type to match the column's type exactly.
    * Rather than touch every one of the ~190 call sites that build these binds as Strings/JsonArrays (that was the
    * whole point of keeping the old JsonArray-based contract, see ADR 0003), convert the well-known shapes back to
    * their proper Java types here.
    */
  private val HasJsonCast = "(?i)::jsonb?\\b".r

  private def toBindValue(value: AnyRef, hasJsonCast: Boolean): AnyRef = value match {
    case s: String if UuidPattern.pattern.matcher(s).matches() =>
      java.util.UUID.fromString(s)

    case s: String if IsoDateTimePattern.pattern.matcher(s).matches() =>
      scala.util.Try(org.joda.time.DateTime.parse(s)) match {
        case scala.util.Success(dt) =>
          java.time.OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(dt.getMillis), java.time.ZoneOffset.UTC)
        case scala.util.Failure(_) => s
      }

    case s: String if IsoDatePattern.pattern.matcher(s).matches() =>
      scala.util.Try(java.time.LocalDate.parse(s)).getOrElse(s)

    // jsonb columns: call sites across the codebase pre-encode JSON as a String (e.g. `someJsonObject.encode()`)
    // and rely on Postgres' `::jsonb` cast to parse it, same as the old client. The reactive client instead
    // JSON-*encodes* whatever Java value it's given for a jsonb-inferred parameter (via its own Json.encode), so a
    // pre-encoded String comes out double-encoded - a JSON string literal containing the real JSON as escaped text,
    // rather than the real JSON structure. That's invisible for a plain read-back of the same value, but breaks any
    // SQL-side JSON operation (e.g. `existing_jsonb || ?::jsonb`, which then treats each side as a lone scalar
    // instead of concatenating array elements). Parsing back into a JsonObject/JsonArray here lets the reactive
    // client's encoder round-trip the real structure instead.
    // Gated on the SQL actually containing a `::jsonb`/`::json` cast: plenty of genuine `text` columns also store
    // JSON-shaped content (e.g. a langtags list serialized as text) and must keep going through as a plain String.
    case s: String if hasJsonCast && s.trim.startsWith("{") =>
      scala.util.Try(new JsonObject(s)).getOrElse(s)

    case s: String if hasJsonCast && s.trim.startsWith("[") =>
      scala.util.Try(new JsonArray(s)).getOrElse(s)

    case arr: JsonArray if isStringArray(arr) => stringArrayOf(arr)

    case other => other
  }

  private def toTuple(params: JsonArray, hasJsonCast: Boolean): Tuple = {
    val values = params.getList.asInstanceOf[java.util.List[Object]].asScala.map(toBindValue(_, hasJsonCast))
    Tuple.tuple(values.asJava)
  }

  private[scala] def runQuery(client: SqlClient, sql: String, params: Option[JsonArray]): Future[RowSet[Row]] = {
    val positionalSql = toPositional(sql)
    val hasJsonCast = HasJsonCast.findFirstIn(sql).isDefined

    FutureHelper.futurify[RowSet[Row]] { (promise: Promise[RowSet[Row]]) =>
      def complete(ar: AsyncResult[RowSet[Row]]): Unit = {
        if (ar.succeeded()) promise.success(ar.result()) else promise.failure(ar.cause())
      }

      params match {
        case Some(p) => client.preparedQuery(positionalSql).execute(toTuple(p, hasJsonCast), complete)
        case None => client.query(positionalSql).execute(complete)
      }
    }
  }
}

class SQLConnection(val vertxAccess: VertxAccess, private val config: JsonObject) extends DatabaseAction {

  override val vertx: Vertx = vertxAccess.vertx

  /**
    * It's non shared, otherwise stopping the verticle will last forever. Test will create many SQLConnection not just
    * the verticle.
    */
  private val poolRef = new AtomicReference[Pool](SQLConnection.pool(vertx, config))

  /**
    * Resets the connection pool by closing all existing connections and creating a new pool. This clears any cached
    * prepared statements that may have become stale after schema changes (e.g., after DROP SCHEMA CASCADE).
    */
  def resetPool(): Future[Unit] = {
    val oldPool = poolRef.getAndSet(SQLConnection.pool(vertx, config))
    oldPool.close()
    Future.successful(())
  }

  /**
    * Vert.x's own `Transaction`/`conn.begin()` API has a known, unfixed ordering issue where commands issued through it
    * can reach Postgres out of order, surfacing as spurious "current transaction is aborted" errors (see
    * eclipse-vertx/vertx-sql-client#312). We avoid that API entirely and drive the transaction with plain
    * `BEGIN`/`COMMIT`/`ROLLBACK` statements against a checked-out connection instead - functionally identical, but
    * without the buggy abstraction in between.
    */
  def transaction(): Future[Transaction] = {
    FutureHelper
      .futurify[JSqlConnection] { (promise: Promise[JSqlConnection]) =>
        poolRef.get().getConnection((ar: AsyncResult[JSqlConnection]) => {
          if (ar.succeeded()) promise.success(ar.result()) else promise.failure(ar.cause())
        })
      }
      .flatMap(conn => SQLConnection.runQuery(conn, "BEGIN", None).map(_ => new Transaction(vertxAccess, conn)))
  }

  override def execute(sql: String): Future[Unit] = {
    wrap { pool =>
      execute(pool, sql)
    }
  }

  override def query(sql: String): Future[RowSet[Row]] = query(sql, None)

  override def query(sql: String, params: JsonArray): Future[RowSet[Row]] = query(sql, Some(params))

  private def query(sql: String, params: Option[JsonArray]): Future[RowSet[Row]] = {
    wrap { pool =>
      query(pool, sql, params)
    }
  }

  override def update(sql: String): Future[RowSet[Row]] = update(sql, None)

  override def update(sql: String, params: JsonArray): Future[RowSet[Row]] = update(sql, Some(params))

  private def update(sql: String, params: Option[JsonArray]): Future[RowSet[Row]] = {
    wrap { pool =>
      update(pool, sql, params)
    }
  }

  private def wrap[A](fn: Pool => Future[A]): Future[A] = {
    val cachedPlanError = "cached plan must not change result type"
    def shouldRetry(ex: Throwable): Boolean =
      NonFatal(ex) && Option(ex.getMessage).exists(_.contains(cachedPlanError))

    val pool = poolRef.get()

    fn(pool).recoverWith({
      case ex: Throwable if shouldRetry(ex) =>
        logger.warn(s"Detected retryable database error. Retrying.", ex)
        fn(pool)
      case ex: Throwable =>
        logger.error(s"Database query/update/execute failed.", ex)
        Future.failed[A](ex)
    })
  }

  def close(): Future[Unit] = {
    poolRef.get().close()
    Future.successful(())
  }
}

class Transaction(val vertxAccess: VertxAccess, private val conn: JSqlConnection) extends DatabaseAction {

  override val vertx: Vertx = vertxAccess.vertx

  // DatabaseConnection.Transaction (database.scala) rolls back on a failed query itself and then the enclosing
  // `transactional` also rolls back the same transaction on failure - the old JDBC-style client tolerated this
  // double rollback/commit silently. Since we now drive commit/rollback with plain SQL text (see SQLConnection
  // .transaction() above), guard against sending a second COMMIT/ROLLBACK (or querying) once the connection has
  // already been closed by the first one.
  private val completed = new AtomicBoolean(false)

  sealed implicit class DatabaseFuture[A](future: Future[A]) {

    def recoverDatabaseException(name: String): Future[A] = {
      future.recoverWith({
        case e =>
          logger.error(s"Database ($name) action failed.", e)
          Future.failed(DatabaseException(e.getMessage, "unknown"))
      })
    }
  }

  override def execute(sql: String): Future[Unit] = {
    execute(conn, sql).recoverDatabaseException("execute")
  }

  override def query(sql: String): Future[RowSet[Row]] = {
    query(conn, sql, None).recoverDatabaseException("query")
  }

  override def query(sql: String, params: JsonArray): Future[RowSet[Row]] = {
    query(conn, sql, Some(params)).recoverDatabaseException("query")
  }

  override def update(sql: String): Future[RowSet[Row]] = {
    update(conn, sql, None).recoverDatabaseException("update")
  }

  override def update(sql: String, params: JsonArray): Future[RowSet[Row]] = {
    update(conn, sql, Some(params)).recoverDatabaseException("update")
  }

  def commit(): Future[Unit] = {
    if (completed.compareAndSet(false, true)) {
      SQLConnection
        .runQuery(conn, "COMMIT", None)
        .flatMap(_ => conn.close())
        .recoverDatabaseException("commit")
    } else {
      Future.successful(())
    }
  }

  def rollback(): Future[Unit] = {
    if (completed.compareAndSet(false, true)) {
      SQLConnection
        .runQuery(conn, "ROLLBACK", None)
        .flatMap(_ => conn.close())
        .recoverDatabaseException("rollback")
    } else {
      Future.successful(())
    }
  }
}
