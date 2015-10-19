package io.vertx.scala

import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, UpdateResult}
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.SQLConnection.JSQLConnection

import scala.concurrent.Future

object SQLConnection {
  type JSQLConnection = io.vertx.ext.sql.SQLConnection

  def apply(vertx: Vertx, config: JsonObject): SQLConnection = {
    new SQLConnection(vertx, config)
  }
}

sealed trait SQLFunction

case class Execute() extends SQLFunction
case class Update() extends SQLFunction
case class Query() extends SQLFunction

sealed trait SQLCommons extends VertxExecutionContext {

  def execute(sql: String): Future[Unit] = {
    for {
      connection <- connection()
      _ <- execute(connection, sql)
    } yield {
      connection.close()
    }
  }

  protected def execute(connection: JSQLConnection, sql: String): Future[Unit] = connection.execute(sql, _: Handler[AsyncResult[Void]])

  def query(sql: String): Future[ResultSet] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = true)
      resultSet <- query(connection, sql)
    } yield {
      connection.close()
      resultSet
    }
  }

  def query(sql: String, params: JsonArray): Future[ResultSet] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = true)
      resultSet <- query(connection, sql, params)
    } yield {
      connection.close()
      resultSet
    }
  }

  protected def query(connection: JSQLConnection, sql: String): Future[ResultSet] = connection.query(sql, _: Handler[AsyncResult[ResultSet]])

  protected def query(connection: JSQLConnection, sql: String, params: JsonArray): Future[ResultSet] = connection.queryWithParams(sql, params, _: Handler[AsyncResult[ResultSet]])

  def update(sql: String): Future[UpdateResult] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = true)
      resultSet <- update(connection, sql)
    } yield {
      connection.close()
      resultSet
    }
  }

  def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    for {
      connection <- connection()
      _ <- setAutoUpdate(connection, autoCommit = true)
      resultSet <- update(connection, sql, params)
    } yield {
      connection.close()
      resultSet
    }
  }

  protected def update(connection: JSQLConnection, sql: String): Future[UpdateResult] = connection.update(sql, _: Handler[AsyncResult[UpdateResult]])

  protected def update(connection: JSQLConnection, sql: String, params: JsonArray): Future[UpdateResult] = connection.updateWithParams(sql, params, _: Handler[AsyncResult[UpdateResult]])

  protected def connection(): Future[JSQLConnection] = getClient.getConnection(_: Handler[AsyncResult[JSQLConnection]])

  protected def close(connection: JSQLConnection): Future[Unit] = connection.close(_: Handler[AsyncResult[Void]])

  protected def setAutoUpdate(connection: JSQLConnection, autoCommit: Boolean): Future[Unit] = connection.setAutoCommit(autoCommit, _: Handler[AsyncResult[Void]])

  protected def getClient: JDBCClient
}

class SQLConnection(val vertx: Vertx, val config: JsonObject) extends SQLCommons {

  val client = JDBCClient.createShared(vertx, config)

  def transaction(): Future[Transaction] = {
    for {
      connection <- connection()
      _ <- connection.setAutoCommit(false, _: Handler[AsyncResult[Void]])
      transaction = new Transaction(connection)
    } yield transaction
  }

  override def getClient: JDBCClient = client
}

class Transaction(val conn: JSQLConnection) extends SQLCommons {

  override def connection(): Future[JSQLConnection] = Future.successful(conn)

  override def execute(sql: String): Future[Unit] = {
    execute(conn, sql)
  }

  override def query(sql: String): Future[ResultSet] = {
    query(conn, sql)
  }

  override def query(sql: String, params: JsonArray): Future[ResultSet] = {
    query(conn, sql, params)
  }

  override def update(sql: String): Future[UpdateResult] = {
    update(conn, sql)
  }

  override def update(sql: String, params: JsonArray): Future[UpdateResult] = {
    update(conn, sql, params)
  }

  def commit(): Future[Unit] = for {
    _ <- conn.commit(_: Handler[AsyncResult[Void]])
    _ <- close(conn)
  } yield ()

  def rollback(): Future[Unit] = for {
    _ <- conn.rollback(_: Handler[AsyncResult[Void]])
    _ <- close(conn)
  } yield ()

  override def getClient: JDBCClient = null
}