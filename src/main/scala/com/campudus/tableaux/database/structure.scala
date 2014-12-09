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

    var result: JsonObject = Json.obj()

    def query(eb: EventBus, query: String, values: JsonArray): Future[Transaction.type] = {
      val jsonQuery = Json.obj(
        "action" -> "prepared",
        "statement" -> query,
        "values" -> values)

      val p = Promise[Transaction.type]
      eb.send(address, jsonQuery, { rep: Message[JsonObject] =>
        result = rep.body()
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

  def createTable(eb: EventBus, name: String): Future[IdType] = for {
    t <- beginTransaction(eb)
    t <- t.query(eb, "INSERT INTO system_table (user_table_names) VALUES (?) RETURNING table_id", Json.arr(name)) recoverWith t.recover(eb)
    id <- Future.successful(t.result.getArray("results").get[JsonArray](0).get[Long](0))
    t <- t.query(eb, s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr()) recoverWith t.recover(eb)
    t <- t.query(eb, s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr()) recoverWith t.recover(eb)
    _ <- t.commit(eb)
  } yield id

  def getTable(eb: EventBus, tableId: IdType): Future[String] = for {
    t <- beginTransaction(eb)
    t <- t.query(eb, "SELECT table_id, user_table_names FROM system_table WHERE table_id = ?", Json.arr(tableId)) recoverWith t.recover(eb)
    n <- Future.successful(t.result.getArray("results").get[JsonArray](0).get[String](1))
    _ <- t.commit(eb)
  } yield n

  def insertColumn(eb: EventBus, tableId: IdType, name: String, columnType: String): Future[Long] = for {
    t <- beginTransaction(eb)
    t <- t.query(eb, s"""
                     |INSERT INTO system_columns 
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id""".stripMargin,
      Json.arr(tableId, name)) recoverWith t.recover(eb)
    id <- Future.successful(t.result.getArray("results").get[JsonArray](0).get[Long](0))
    t <- t.query(eb, s"ALTER TABLE user_table_$tableId ADD column_$id $columnType)", Json.arr()) recoverWith t.recover(eb)
    _ <- t.commit(eb)
  } yield id

  def beginTransaction(eb: EventBus): Future[Transaction.type] = {
    val p = Promise[Transaction.type]
    eb.send(address, Json.obj("action" -> "begin"), { rep: Message[JsonObject] =>
      p.success(Transaction)
    })
    p.future
  }

}

class Tableaux(verticle: Verticle) {

  val vertx = verticle.vertx

  def create(name: String): Future[Table] = for {
    id <- createTable(vertx.eventBus, name)
  } yield new Table(id, name, List())

  def delete(id: IdType): Future[Unit] = ???

  def addColumn[T <: ColumnType](tableId: IdType, name: String, columnType: String): Future[T] = for {
    table <- getTable(tableId)
    id <- insertColumn(vertx.eventBus, table.id, name, columnType)
  } yield {
    columnType match {
      case "text"    => StringColumn(table, id, name)
      case "numeric" => NumberColumn(table, id, name)
    }
  }.asInstanceOf[T]

  def insertValue[T <: ColumnType](tableId: IdType, columnId: IdType, rowId: IdType, value: T): Future[Unit] = for {
    table <- getTable(tableId)
    column <- table.getColumn(columnId)
    v: column.Value <- Future.apply(value.asInstanceOf[column.Value])
    _ <- column.setValue(rowId, v)
  } yield ()

  def getTable(tableId: IdType): Future[Table] = for {
    name <- TableStructure.getTable(vertx.eventBus, tableId)
  } yield Table(tableId, name, List())

  def getColumn(columnId: IdType): Future[ColumnType] = ???

}
