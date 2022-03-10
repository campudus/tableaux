package io.vertx.scala

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.scala.SQLConnection.JSQLConnection
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.asyncsql.PostgreSQLClient
import io.vertx.scala.ext.sql.{ResultSet, UpdateResult}
import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.concurrent.Future

sealed trait DatabaseAction extends VertxAccess {

  def execute(sql: String): Future[Unit]

  protected def execute(connection: JSQLConnection, sql: String): Future[Unit] = connection.executeFuture(sql)

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

  protected def close(connection: JSQLConnection): Future[Unit] = connection.closeFuture()

  protected def setAutoUpdate(connection: JSQLConnection, autoCommit: Boolean): Future[Unit] =
    connection.setAutoCommitFuture(autoCommit)
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
    * It's non shared, otherwise stopping the verticle will last forever. Test will create many SQLConnection not just
    * the verticle.
    */
  private val client = PostgreSQLClient.createNonShared(vertxAccess.vertx, config)

  def transaction(): Future[Transaction] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = false)
    } yield new Transaction(vertxAccess, connection)
  }

  override protected def connection(): Future[JSQLConnection] = client.getConnectionFuture()

  override def execute(sql: String): Future[Unit] = {
    wrap { conn =>
      execute(conn, sql)
    }
  }

  override def query(sql: String): Future[ResultSet] = query(sql, None)

  override def query(sql: String, params: JsonArray): Future[ResultSet] = query(sql, Some(params))

  private def query(sql: String, params: Option[JsonArray]): Future[ResultSet] = {
    wrap { conn =>
      query(conn, sql, params)
    }
  }

  override def update(sql: String): Future[UpdateResult] = update(sql, None)

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = update(sql, Some(params))

  private def update(sql: String, params: Option[JsonArray]): Future[UpdateResult] = {
    wrap { conn =>
      update(conn, sql, params)
    }
  }

  private def wrap[A](fn: (JSQLConnection) => Future[A]): Future[A] = {
    for {
      conn <- connection()
      result <- fn(conn).recoverWith({
        case ex: Throwable =>
          logger.error(s"Database query/update/execute failed. Close connection.", ex)
          close(conn).flatMap(_ => Future.failed[A](ex))
      })
      _ <- close(conn)
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
          logger.error(s"Database ($name) action failed.", e)
          Future.failed(DatabaseException(e.getMessage, "unknown"))
      })
    }
  }

  override def execute(sql: String): Future[Unit] = {
    execute(conn, sql).recoverDatabaseException("execute")
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
    (for {
      _ <- conn.commitFuture()
      _ <- close(conn)
    } yield ()).recoverDatabaseException("commit")
  }

  def rollback(): Future[Unit] = {
    (for {
      _ <- conn.rollbackFuture()
      _ <- close(conn)
    } yield ()).recoverDatabaseException("rollback")
  }
}
