package com.campudus.tableaux.database

import scala.concurrent.{ Future, Promise }
import org.vertx.scala.core.VertxExecutionContext
import TableStructure._
import com.campudus.tableaux.Starter
import org.vertx.scala.core.json.{ Json, JsonObject, JsonArray }
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.Vertx
import org.vertx.scala.platform.Verticle
import org.vertx.scala.core.eventbus.EventBus

class ExecutionContext() {
  val verticle: Starter = verticle
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)
}

sealed trait ColumnType {
  type Value

  def columnId: IdType

  def name: String

  def table: Table

  def setValue(rowId: IdType, value: Value): Future[Unit] = ???
}

case class StringColumn(table: Table, columnId: IdType, name: String) extends ColumnType {
  type Value = String
}

case class NumberColumn(table: Table, columnId: IdType, name: String) extends ColumnType {
  type Value = Number
}

case class LinkColumn(table: Table, columnId: IdType, to: IdType, name: String) extends ColumnType {
  type Value = Link
}

case class Link(value: Seq[IdType])

case class Table(id: IdType, name: String, columns: Seq[ColumnType]) {
  def getColumn(columnId: IdType): Future[ColumnType] = {
    Future.apply(columns.find(_.columnId == columnId).get)
  }
}

object TableStructure extends ExecutionContext {

  type IdType = Long

  val address = "campudus.asyncdb"

  case object Transaction {
    
    def query(eb: EventBus, query: String, values: JsonArray): Future[Transaction.type] = {
      val jsonQuery = Json.obj(
        "action" -> "prepared",
        "statement" -> query,
        "values" -> values)

      val p = Promise[Transaction.type]
      eb.send(address, jsonQuery, { rep: Message[JsonObject] =>
        println(rep.body().encode())
        p.success(this)
      })
      p.future
    }

    def commit(eb: EventBus): Future[Unit] = {
      val p = Promise[Unit]
      eb.send(address, Json.obj("action" -> "commit"), { rep: Message[JsonObject] =>
        p.success()
      })
      p.future
    }

    def rollback(eb: EventBus): Future[Unit] = {
      val p = Promise[Unit]
      eb.send(address, Json.obj("action" -> "rollback"), { rep: Message[JsonObject] =>
        p.success()
      })
      p.future
    }

    def recover(eb: EventBus): PartialFunction[Throwable, Future[Transaction.type]] = {
      case ex: Throwable => rollback(eb: EventBus) flatMap (_ => Future.failed[Transaction.type](ex))
    }
  }

  def deinstall(eb: EventBus): Future[Unit] = for {
    t <- beginTransaction(eb)
    t <- t.query(eb, s"""DROP SCHEMA public CASCADE""".stripMargin, Json.arr()) recoverWith t.recover(eb)
    t <- t.query(eb, s"""CREATE SCHEMA public""".stripMargin, Json.arr()) recoverWith t.recover(eb)
    _ <- t.commit(eb)
  } yield ()

  def setup(eb: EventBus): Future[Unit] = for {
    t <- beginTransaction(eb)
    t <- t.query(eb, s"""
                     |CREATE TABLE system_table (
                     |  table_id BIGSERIAL,
                     |  user_table_names VARCHAR(255) NOT NULL,
                     |  PRIMARY KEY(table_id)
                     |)""".stripMargin, 
                     Json.arr()) recoverWith t.recover(eb)
    t <- t.query(eb, s"""
                     |CREATE TABLE system_columns(
                     |  table_id BIGINT,
                     |  column_id BIGINT,
                     |  user_column_name VARCHAR(255) NOT NULL,
                     |  ordering BIGINT NOT NULL,
                     |  link_id BIGINT,
                     |
                     |  PRIMARY KEY(table_id, column_id),
                     |  FOREIGN KEY(table_id)
                     |  REFERENCES system_table(table_id)
                     |  ON DELETE CASCADE
                     |)""".stripMargin,
                     Json.arr()) recoverWith t.recover(eb)
    t <- t.query(eb, s"""
                     |CREATE TABLE system_link_table(
                     |  link_id BIGSERIAL,
                     |  table_id_1 BIGINT,
                     |  table_id_2 BIGINT,
                     |  column_id_1 BIGINT,
                     |  column_id_2 BIGINT,
                     |
                     |  PRIMARY KEY(link_id),
                     |  FOREIGN KEY(table_id_1, column_id_1)
                     |  REFERENCES system_columns(table_id, column_id)
                     |  ON DELETE CASCADE,
                     |  FOREIGN KEY(table_id_2, column_id_2)
                     |  REFERENCES system_columns(table_id, column_id)
                     |  ON DELETE CASCADE
                     |)""".stripMargin,
                     Json.arr()) recoverWith t.recover(eb)
    t <- t.query(eb, s"""
                     |ALTER TABLE system_columns
                     |  ADD FOREIGN KEY(link_id)
                     |  REFERENCES system_link_table(link_id)
                     |  ON DELETE CASCADE""".stripMargin,
                     Json.arr()) recoverWith t.recover(eb)
    _ <- t.commit(eb)
  } yield ()

  def beginTransaction(eb: EventBus): Future[Transaction.type] = {
    val p = Promise[Transaction.type]
    eb.send(address, Json.obj("action" -> "begin"), { rep: Message[JsonObject] =>
      p.success(Transaction)
    })
    p.future
  }

}

class Tableaux(verticle: Verticle) {

  val address = "campudus.asyncdb"
  val vertx = verticle.vertx

  private def ebSend(json: JsonObject): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    vertx.eventBus.send(address, json, { rep: Message[JsonObject] =>
      println(rep.body().encode())
      p.success(rep.body())
    })
    p.future
  }

  def create(name: String): Future[Table] = {
    val jsonInsert = Json.obj(
      "action" -> "prepared",
      "statement" -> "INSERT INTO system_table (user_table_names) VALUES (?) RETURNING table_id",
      "values" -> Json.arr(name))

    def jsonCreate(id: Long) = Json.obj(
      "action" -> "raw",
      "command" -> s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))")

    for {
      id <- ebSend(jsonInsert) map { r => r.getArray("results").get[JsonArray](0).get[Long](0) }
      json <- Future.successful(jsonCreate(1))
      x <- ebSend(json)
    } yield new Table(id, name, List())
  }

  def delete(id: IdType): Future[Unit] = ???

  def addColumn[T <: ColumnType](tableId: IdType, name: String): Future[T] = for {
    table <- getTable(tableId)
  } yield {
    ???
  }

  def insertValue[T <: ColumnType](tableId: IdType, columnId: IdType, rowId: IdType, value: T): Future[Unit] = for {
    table <- getTable(tableId)
    column <- table.getColumn(columnId)
    v: column.Value <- Future.apply(value.asInstanceOf[column.Value])
    _ <- column.setValue(rowId, v)
  } yield ()

  def getTable(tableId: IdType): Future[Table] = {
    val json = Json.obj(
      "action" -> "prepared",
      "statement" -> "SELECT table_id, user_table_names FROM system_table WHERE user_table_id = ?",
      "values" -> Json.arr(tableId))

    for {
      r <- ebSend(json)
      name <- Future.successful(r.getArray("results").get[JsonArray](0).get[String](1))
    } yield new Table(tableId, name, List())
  }

  def getColumn(columnId: IdType): Future[ColumnType] = ???

}
