package io.vertx.scala

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.VertxAccess
import org.vertx.scala.core.json.{JsonArray, JsonObject}
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.ext.asyncsql.PostgreSQLClient
import io.vertx.ext.sql.{ResultSet, UpdateResult}
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.SQLConnection.JSQLConnection

import scala.concurrent.Future

sealed trait DatabaseAction extends VertxAccess {

  def execute(sql: String): Future[Unit]

  protected def execute(connection: JSQLConnection, sql: String): Future[Unit] = {
    connection.execute(sql, _: Handler[AsyncResult[Void]])
  }

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

  protected def setAutoUpdate(connection: JSQLConnection, autoCommit: Boolean): Future[Unit] = {
    connection.setAutoCommit(autoCommit, _: Handler[AsyncResult[Void]])
  }
}

object SQLConnection {
  val QUERY_TIMEOUT = 5000
  val LEASE_TIMEOUT = 10000

  type JSQLConnection = io.vertx.ext.sql.SQLConnection

  def apply(verticle: ScalaVerticle, config: JsonObject): SQLConnection = {
    new SQLConnection(verticle, config)
  }
}

class SQLConnection(val verticle: ScalaVerticle, private val config: JsonObject) extends DatabaseAction {

  import com.campudus.tableaux.helper.TimeoutScheduler._

  // It's non shared, otherwise stopping the verticle will last forever.
  // Test will create many SQLConnection not just the verticle
  val client = PostgreSQLClient.createNonShared(vertx, config)

  def transaction(): Future[Transaction] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = false).withTimeout(SQLConnection.QUERY_TIMEOUT, "transaction")
    } yield new Transaction(verticle, connection)
  }

  private def connect(): Future[JSQLConnection] = client.getConnection(_: Handler[AsyncResult[JSQLConnection]])

  override protected def connection(): Future[JSQLConnection] = {
    connect().withTimeout(SQLConnection.QUERY_TIMEOUT, "connect")
  }

  override def execute(sql: String): Future[Unit] = {
    wrap{ conn => {
      for {
        _ <- execute(conn, sql).withTimeout(SQLConnection.QUERY_TIMEOUT, "execute")
      } yield ()
    }
    }
  }

  override def query(sql: String): Future[ResultSet] = query(sql, None)

  override def query(sql: String, params: JsonArray): Future[ResultSet] = query(sql, Some(params))

  private def query(sql: String, params: Option[JsonArray]): Future[ResultSet] = {
    wrap{ conn => {
      for {
        resultSet <- query(conn, sql, params).withTimeout(SQLConnection.QUERY_TIMEOUT, "query")
      } yield {
        resultSet
      }
    }
    }
  }

  override def update(sql: String): Future[UpdateResult] = update(sql, None)

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = update(sql, Some(params))

  private def update(sql: String, params: Option[JsonArray]): Future[UpdateResult] = {
    wrap{ conn => {
      for {
        updateResult <- update(conn, sql, params).withTimeout(SQLConnection.QUERY_TIMEOUT, "update")
      } yield {
        updateResult
      }
    }
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

  val connectionTimerId = vertx.setTimer(SQLConnection.LEASE_TIMEOUT, { d: java.lang.Long => {
    logger.error(s"Lease timeout exceeded")
    conn.close()
  }
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
      .withTimeout(SQLConnection.QUERY_TIMEOUT, "execute")
  }

  override def query(sql: String): Future[ResultSet] = {
    query(conn, sql, None)
      .recoverDatabaseException("query")
      .withTimeout(SQLConnection.QUERY_TIMEOUT, "query")
  }

  override def query(sql: String, params: JsonArray): Future[ResultSet] = {
    query(conn, sql, Some(params))
      .recoverDatabaseException("query")
      .withTimeout(SQLConnection.QUERY_TIMEOUT, "query")
  }

  override def update(sql: String): Future[UpdateResult] = {
    update(conn, sql, None)
      .recoverDatabaseException("update")
      .withTimeout(SQLConnection.QUERY_TIMEOUT, "update")
  }

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    update(conn, sql, Some(params))
      .recoverDatabaseException("update")
      .withTimeout(SQLConnection.QUERY_TIMEOUT, "update")
  }

  override def close(_conn: JSQLConnection): Future[Unit] = {
    vertx.cancelTimer(connectionTimerId)
    super.close(_conn)
  }

  def commit(): Future[Unit] = {
    (for {
      _ <- asyncVoidToFuture(conn.commit(_: AsyncVoid)).withTimeout(SQLConnection.QUERY_TIMEOUT, "commit")
      _ <- close(conn)
    } yield ()) recoverDatabaseException "commit" recoverWith {
      case e =>
        rollback()
        Future.failed(e)
    } withTimeout(SQLConnection.QUERY_TIMEOUT, "commit")
  }

  def rollback(): Future[Unit] = {
    (for {
      _ <- asyncVoidToFuture(conn.rollback(_: Handler[AsyncResult[Void]]))
        .withTimeout(SQLConnection.QUERY_TIMEOUT, "rollback")
      _ <- close(conn)
    } yield {
      ()
    }) recoverDatabaseException "rollback" withTimeout(SQLConnection.QUERY_TIMEOUT, "rollback")
  }
}
