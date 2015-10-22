package io.vertx.scala

import com.campudus.tableaux.DatabaseException
import com.campudus.tableaux.helper.StandardVerticle
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.ext.asyncsql.{AsyncSQLClient, PostgreSQLClient}
import io.vertx.ext.sql.{ResultSet, UpdateResult}
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.SQLConnection.JSQLConnection

import scala.concurrent.Future

object SQLConnection {
  val LEASE_TIMEOUT = 10000

  type JSQLConnection = io.vertx.ext.sql.SQLConnection

  def apply(verticle: ScalaVerticle, config: JsonObject): SQLConnection = {
    new SQLConnection(verticle, config)
  }
}

sealed trait SQLFunction

case class Execute() extends SQLFunction

case class Update() extends SQLFunction

case class Query() extends SQLFunction

sealed trait SQLCommons extends StandardVerticle {

  def execute(sql: String): Future[Unit] = {
    for {
      connection <- connection()
      _ <- execute(connection, sql)
      _ <- close(connection)
    } yield ()
  }

  protected def close(connection: JSQLConnection): Future[Unit] = connection.close(_: Handler[AsyncResult[Void]])

  protected def execute(connection: JSQLConnection, sql: String): Future[Unit] = connection.execute(sql, _: Handler[AsyncResult[Void]])

  def query(sql: String): Future[ResultSet] = {
    for {
      conn <- connection()
      resultSet <- query(conn, sql)
      _ <- close(conn)
    } yield {
      resultSet
    }
  }

  def query(sql: String, params: JsonArray): Future[ResultSet] = {
    for {
      conn <- connection()
      resultSet <- query(conn, sql, params)
      _ <- close(conn)
    } yield {
      resultSet
    }
  }

  protected def query(connection: JSQLConnection, sql: String): Future[ResultSet] = connection.query(sql, _: Handler[AsyncResult[ResultSet]])

  protected def query(connection: JSQLConnection, sql: String, params: JsonArray): Future[ResultSet] = connection.queryWithParams(sql, params, _: Handler[AsyncResult[ResultSet]])

  def update(sql: String): Future[UpdateResult] = {
    for {
      conn <- connection()
      updateResult <- update(conn, sql)
      _ <- close(conn)
    } yield {
      updateResult
    }
  }

  def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    for {
      conn <- connection()
      updateResult <- update(conn, sql, params)
      _ <- close(conn)
    } yield {
      updateResult
    }
  }

  protected def update(connection: JSQLConnection, sql: String): Future[UpdateResult] = connection.update(sql, _: Handler[AsyncResult[UpdateResult]])

  protected def update(connection: JSQLConnection, sql: String, params: JsonArray): Future[UpdateResult] = connection.updateWithParams(sql, params, _: Handler[AsyncResult[UpdateResult]])

  protected def connection(): Future[JSQLConnection] = getClient.getConnection(_: Handler[AsyncResult[JSQLConnection]])

  protected def setAutoUpdate(connection: JSQLConnection, autoCommit: Boolean): Future[Unit] = connection.setAutoCommit(autoCommit, _: Handler[AsyncResult[Void]])

  protected def getClient: AsyncSQLClient
}

class SQLConnection(val verticle: ScalaVerticle, val config: JsonObject) extends SQLCommons {
  val client = PostgreSQLClient.createShared(vertx, config)

  def transaction(): Future[Transaction] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = false)
    } yield new Transaction(verticle, connection)
  }

  override def getClient: AsyncSQLClient = client
}

class Transaction(val verticle: ScalaVerticle, val conn: JSQLConnection) extends SQLCommons {

  import com.campudus.tableaux.TimeoutScheduler._

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
    execute(conn, sql).recoverDatabaseException("execute").withTimeout(10000, "execute")
  }

  override def query(sql: String): Future[ResultSet] = {
    query(conn, sql).recoverDatabaseException("query").withTimeout(10000, "query")
  }

  override def query(sql: String, params: JsonArray): Future[ResultSet] = {
    query(conn, sql, params).recoverDatabaseException("query").withTimeout(10000, "query")
  }

  override def update(sql: String): Future[UpdateResult] = {
    update(conn, sql).recoverDatabaseException("update").withTimeout(10000, "update")
  }

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    update(conn, sql, params).recoverDatabaseException("update").withTimeout(10000, "update")
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
    import com.campudus.tableaux.TimeoutScheduler._

    import scala.concurrent.duration.DurationInt

    (for {
      _ <- asyncVoid(conn.rollback(_: Handler[AsyncResult[Void]])).withTimeout(DurationInt(1).seconds, "rollback")
      _ <- close(conn)
    } yield {
        ()
      }) recoverDatabaseException "rollback" withTimeout(10000, "commit")
  }

  override def getClient: AsyncSQLClient = null
}