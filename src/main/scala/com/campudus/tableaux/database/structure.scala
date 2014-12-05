package com.campudus.tableaux.database

import scala.concurrent.{ Future, Promise }
import org.vertx.scala.core.VertxExecutionContext
import TableStructure._
import com.campudus.tableaux.Starter
import org.vertx.scala.core.json.Json
import org.vertx.java.core.json.JsonObject
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.Vertx

trait ExecutionContext {
  val verticle: Starter
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

case class Table(id: IdType, name: String, columns: Seq[ColumnType]) extends ExecutionContext {
  def getColumn(columnId: IdType): Future[ColumnType] = {
    Future.apply(columns.find(_.columnId == columnId).get)
  }
}

object TableStructure extends ExecutionContext {

  type IdType = Long

  case object Transaction {

    def query(query: String): Future[Transaction.type] = ???

    def commit(): Future[Unit] = ???

    def rollback(): Future[Unit] = ???

    def recover(): PartialFunction[Throwable, Future[Transaction.type]] = {
      case ex: Throwable => rollback() flatMap (_ => Future.failed[Transaction.type](ex))
    }
  }

  def deinstall(): Future[Unit] = ???

  def setup(): Future[Unit] = for {
    t <- beginTransaction()
    t <- t.query(s"""
                     |CREATE TABLE system_table (
                     |  table_id BIGSERIAL,
                     |  user_table_names VARCHAR(255) NOT NULL,
                     |  PRIMARY KEY(table_id)
                     |)""".stripMargin) recoverWith t.recover()
    t <- t.query(s"""
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
                     |)""".stripMargin) recoverWith t.recover()
    t <- t.query(s"""
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
                     |)""".stripMargin) recoverWith t.recover()
    t <- t.query(s"""
                     |ALTER TABLE system_columns
                     |  ADD FOREIGN KEY(link_id)
                     |  REFERENCES system_link_table(link_id)
                     |  ON DELETE CASCADE""".stripMargin) recoverWith t.recover()
    _ <- t.commit()
  } yield ()

  def beginTransaction(): Future[Transaction.type] = ???

}

object Table {

  val address = "campudus.asyncdb"
  var id = 1

  private def ebSend(vertx: Vertx)(json: JsonObject): Future[Message[JsonObject]] = {
    val p = Promise[Message[JsonObject]]()
    vertx.eventBus.send(address, json, { rep: Message[JsonObject] =>
      println(rep.body().encode())
      p.success(rep)
    })
    p.future
  }

  // Need change -> SQL Statement
  def create(name: String, vertx: Vertx): Future[Table] = {
    val json = Json.obj(
      "action" -> "raw",
      "command" -> s"CREATE TABLE user_table_1 (id BIGSERIAL, PRIMARY KEY (id))")

    val ebSender = ebSend(vertx) _
    id += 1
    ebSender(json) map { reply =>
      new Table(id, name, List()) // not rdy
    }
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

  def getTable(tableId: IdType): Future[Table] = ???

  def getColumn(columnId: IdType): Future[ColumnType] = ???

}
