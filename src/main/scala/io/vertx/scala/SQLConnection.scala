package io.vertx.scala

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.ext.asyncsql.PostgreSQLClient
import io.vertx.ext.sql.{ResultSet, UpdateResult}
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.SQLConnection.JSQLConnection

import scala.concurrent.Future

sealed trait DatabaseAction extends VertxAccess {

  def execute(sql: String): Future[Unit]

  protected def execute(connection: JSQLConnection, sql: String): Future[Unit] = connection.execute(sql, _: Handler[AsyncResult[Void]])

  def query(sql: String): Future[ResultSet]

  def query(sql: String, params: JsonArray): Future[ResultSet]

  protected def query(connection: JSQLConnection, sql: String, params: Option[JsonArray]): Future[ResultSet] = {
    params match {
      case Some(p) => connection.queryWithParams(sql, p, _: Handler[AsyncResult[ResultSet]])
      case None => connection.query(sql, _: Handler[AsyncResult[ResultSet]])
    }
  }

  def update(sql: String): Future[UpdateResult]

  def update(sql: String, params: JsonArray): Future[UpdateResult]

  protected def update(connection: JSQLConnection, sql: String, params: Option[JsonArray]): Future[UpdateResult] = {
    params match {
      case Some(p) => connection.updateWithParams(sql, p, _: Handler[AsyncResult[UpdateResult]])
      case None => connection.update(sql, _: Handler[AsyncResult[UpdateResult]])
    }
  }

  protected def connection(): Future[JSQLConnection]

  protected def close(connection: JSQLConnection): Future[Unit] = connection.close(_: Handler[AsyncResult[Void]])

  protected def setAutoUpdate(connection: JSQLConnection, autoCommit: Boolean): Future[Unit] = connection.setAutoCommit(autoCommit, _: Handler[AsyncResult[Void]])
}

object SQLConnection {
  val LEASE_TIMEOUT = 10000

  type JSQLConnection = io.vertx.ext.sql.SQLConnection

  def apply(verticle: ScalaVerticle, config: JsonObject): SQLConnection = {
    new SQLConnection(verticle, config)
  }
}

class SQLConnection(val verticle: ScalaVerticle, private val config: JsonObject) extends DatabaseAction {
  val client = PostgreSQLClient.createShared(vertx, config)

  def transaction(): Future[Transaction] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = false)
    } yield new Transaction(verticle, connection)
  }

  override protected def connection(): Future[JSQLConnection] = client.getConnection(_: Handler[AsyncResult[JSQLConnection]])

  override def execute(sql: String): Future[Unit] = wrap {
    conn =>
      for {
        _ <- execute(conn, sql)
      } yield ()
  }

  override def query(sql: String): Future[ResultSet] = query(sql, None)

  override def query(sql: String, params: JsonArray): Future[ResultSet] = query(sql, Some(params))

  private def query(sql: String, params: Option[JsonArray]): Future[ResultSet] = wrap {
    conn =>
      for {
        resultSet <- query(conn, sql, params)
      } yield {
        resultSet
      }
  }

  override def update(sql: String): Future[UpdateResult] = update(sql, None)

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = update(sql, Some(params))

  private def update(sql: String, params: Option[JsonArray]): Future[UpdateResult] = wrap {
    conn =>
      for {
        updateResult <- update(conn, sql, params)
      } yield {
        updateResult
      }
  }

  private def wrap[A](fn: (JSQLConnection) => Future[A]): Future[A] = {
    for {
      conn <- connection()
      result <- fn(conn).recoverWith({
        case ex: Throwable =>
          logger.error(s"Database query/update/execute failed. Close connection.", ex)
          close(conn) flatMap (_ => Future.failed[A](ex))
      })
      _ <- close(conn)
    } yield {
      result
    }
  }

  def close(): Future[Unit] = client.close(_: Handler[AsyncResult[Void]])
}

class Transaction(val verticle: ScalaVerticle, private val conn: JSQLConnection) extends DatabaseAction {

  import com.campudus.tableaux.helper.TimeoutScheduler._

  import scala.concurrent.duration.DurationInt

  val connectionTimerId = vertx.setTimer(SQLConnection.LEASE_TIMEOUT, {
    d: java.lang.Long =>
      logger.error(s"Lease timeout exceeded")
      conn.close()
  })

  override def connection(): Future[JSQLConnection] = Future.successful(conn)

  implicit class DatabaseFuture[A](future: Future[A]) {
    def recoverDatabaseException(name: String): Future[A] = {
      future.recoverWith {
        case e =>
          logger.error(s"Database ($name) action failed.", e)
          Future.failed(DatabaseException(e.getMessage, "unknown"))
      }
    }
  }

  override def execute(sql: String): Future[Unit] = {
    execute(conn, sql)
      .recoverDatabaseException("execute")
      .withTimeout(10000, "execute")
  }

  override def query(sql: String): Future[ResultSet] = {
    query(conn, sql, None)
      .recoverDatabaseException("query")
      .withTimeout(10000, "query")
  }

  override def query(sql: String, params: JsonArray): Future[ResultSet] = {
    query(conn, sql, Some(params))
      .recoverDatabaseException("query")
      .withTimeout(10000, "query")
  }

  override def update(sql: String): Future[UpdateResult] = {
    update(conn, sql, None)
      .recoverDatabaseException("update")
      .withTimeout(10000, "update")
  }

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    update(conn, sql, Some(params))
      .recoverDatabaseException("update")
      .withTimeout(10000, "update")
  }

  override def close(_conn: JSQLConnection): Future[Unit] = {
    vertx.cancelTimer(connectionTimerId)
    super.close(_conn)
  }

  def commit(): Future[Unit] = {
    (for {
      t <- asyncVoid(conn.commit(_: Handler[AsyncResult[Void]])).withTimeout(DurationInt(1).seconds, "commit")
      b <- close(conn)
    } yield {
      ()
    }) recoverDatabaseException "commit" recoverWith {
      case e =>
        rollback()
        Future.failed(e)
    } withTimeout(10000, "commit")
  }

  def rollback(): Future[Unit] = {
    (for {
      _ <- asyncVoid(conn.rollback(_: Handler[AsyncResult[Void]])).withTimeout(DurationInt(1).seconds, "rollback")
      _ <- close(conn)
    } yield {
      ()
    }) recoverDatabaseException "rollback" withTimeout(10000, "commit")
  }
}