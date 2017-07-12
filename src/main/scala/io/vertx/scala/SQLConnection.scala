package io.vertx.scala

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.VertxAccess
import org.vertx.scala.core.json.{JsonArray, JsonObject}
import io.vertx.scala.ext.asyncsql.PostgreSQLClient
import io.vertx.scala.ext.sql.{ResultSet, UpdateResult}
import io.vertx.scala.SQLConnection.JSQLConnection
import io.vertx.scala.core.Vertx

import scala.concurrent.{ExecutionContext, Future}
import io.vertx.lang.scala.ScalaVerticle

sealed trait DatabaseAction extends VertxAccess {

  def execute(sql: String): Future[Unit]

  def query(sql: String): Future[ResultSet]

  def query(sql: String, params: JsonArray): Future[ResultSet]

  protected def query(connection: JSQLConnection, sql: String, params: Option[JsonArray]): Future[ResultSet] = {
    params match {
      case Some(p) => connection.queryWithParamsFuture(sql, p)
      case None => connection.queryFuture(sql)
    }
  }

  def update(sql: String): Future[UpdateResult]

  def update(sql: String, params: JsonArray): Future[UpdateResult]

  protected def update(connection: JSQLConnection, sql: String, params: Option[JsonArray]): Future[UpdateResult] = {
    params match {
      case Some(p) => connection.updateWithParamsFuture(sql, p)
      case None => connection.updateFuture(sql)
    }
  }

  protected def connection(): Future[JSQLConnection]
}

object SQLConnection {
  type JSQLConnection = io.vertx.scala.ext.sql.SQLConnection

  def apply(vertxAccess: VertxAccess, config: JsonObject): SQLConnection = {
    new SQLConnection(vertxAccess, config)
  }
}

class SQLConnection(val vertxAccess: VertxAccess, private val config: JsonObject) extends DatabaseAction {

  override val vertx: Vertx = vertxAccess.vertx

  /**
    * It's non shared, otherwise stopping the verticle will last forever.
    * Test will create many SQLConnection not just the verticle.
    */
  private val client = PostgreSQLClient.createNonShared(vertxAccess.vertx, config)

  def transaction(): Future[Transaction] = {
    wrap("transaction", { conn =>
      conn
        .setAutoCommitFuture(autoCommit = false)
        .map(_ => new Transaction(vertxAccess, conn))
    })
  }

  override protected def connection(): Future[JSQLConnection] = client.getConnectionFuture()

  override def execute(sql: String): Future[Unit] = {
    wrap("execute", { conn =>
      conn.executeFuture(sql)
    })
  }

  override def query(sql: String): Future[ResultSet] = query(sql, None)

  override def query(sql: String, params: JsonArray): Future[ResultSet] = query(sql, Some(params))

  private def query(sql: String, params: Option[JsonArray]): Future[ResultSet] = {
    wrap("query", { conn =>
      query(conn, sql, params)
    })
  }

  override def update(sql: String): Future[UpdateResult] = update(sql, None)

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = update(sql, Some(params))

  private def update(sql: String, params: Option[JsonArray]): Future[UpdateResult] = {
    wrap("update", { conn =>
      update(conn, sql, params)
    })
  }

  private def wrap[A](name: String, fn: (JSQLConnection) => Future[A]): Future[A] = {
    for {
      conn <- connection()

      result <- fn(conn).recoverWith({
        case ex: Throwable =>
          logger.error(s"Database action $name failed. Connection will be closed.", ex)

          // fail quietly and propagate the real error
          conn
            .closeFuture()
            .recoverWith({
              case _ => Future.successful(())
            })
            .flatMap(_ => Future.failed[A](ex))
      })

      _ <- conn
        .closeFuture()
        .recoverWith({
          case _ => Future.successful(())
        })
    } yield result
  }

  def close(): Future[Unit] = client.closeFuture()
}

class Transaction(val vertxAccess: VertxAccess, private val conn: JSQLConnection) extends DatabaseAction {

  override val vertx: Vertx = vertxAccess.vertx

  override def connection(): Future[JSQLConnection] = Future.successful(conn)

  sealed implicit class DatabaseFuture[A](future: Future[A]) {

    def recoverDatabaseException(name: String): Future[A] = {
      future.recoverWith({
        case e =>
          logger.error(s"Action $name in Transaction failed", e)
          Future.failed(DatabaseException(e.getMessage, "unknown"))
      })
    }
  }

  override def execute(sql: String): Future[Unit] = {
    conn.executeFuture(sql).recoverDatabaseException("execute")
  }

  override def query(sql: String): Future[ResultSet] = {
    query(conn, sql, None).recoverDatabaseException("query")
  }

  override def query(sql: String, params: JsonArray): Future[ResultSet] = {
    query(conn, sql, Some(params)).recoverDatabaseException("query")
  }

  override def update(sql: String): Future[UpdateResult] = {
    update(conn, sql, None).recoverDatabaseException("update")
  }

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    update(conn, sql, Some(params)).recoverDatabaseException("update")
  }

  def commit(): Future[Unit] = {
    for {
      _ <- conn
        .commitFuture()
        .recoverDatabaseException("commit")

      // don't fail on close
      _ <- conn
        .closeFuture()
        .recoverDatabaseException("close")
        .recoverWith({
          case _ => Future.successful(())
        })
    } yield ()
  }

  def rollback(): Future[Unit] = {
    for {
      // don't fail on rollback
      _ <- conn
        .rollbackFuture()
        .recoverWith({
          case ex: IllegalStateException =>
            // Ignore if not in transaction currently
            Future.successful(())
        })
        .recoverDatabaseException("rollback")
        .recoverWith({
          case _ => Future.successful(())
        })

      // don't fail on close
      _ <- conn
        .closeFuture()
        .recoverDatabaseException("close")
        .recoverWith({
          case _ => Future.successful(())
        })
    } yield ()
  }
}
