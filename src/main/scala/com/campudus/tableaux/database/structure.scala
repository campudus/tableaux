package com.campudus.tableaux.database

import org.vertx.scala.core.FunctionConverters._
import scala.concurrent.{ Future, Promise }
import org.vertx.scala.core.VertxExecutionContext
import TableStructure._
import org.vertx.scala.core.json.{ Json, JsonObject, JsonArray }
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.Vertx
import org.vertx.scala.platform.Verticle
import org.vertx.scala.core.eventbus.EventBus
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.concurrent.ExecutionContext
import com.campudus.tableaux.Transaction

sealed trait ColumnType {
  type Value

  def columnId: Option[IdType]

  def name: String

  def table: Table

  def setValue(rowId: IdType, value: Value): Future[Unit] = ???

}

sealed trait ValueColumnType extends ColumnType {
  def dbType: String
  def withNewColumnId(colId: IdType): ValueColumnType
}

case class StringColumn(table: Table, columnId: Option[IdType], name: String) extends ValueColumnType {
  type Value = String
  val dbType = "text"
  override def withNewColumnId(colId: IdType) = copy(columnId = Some(colId))
}

case class NumberColumn(table: Table, columnId: Option[IdType], name: String) extends ValueColumnType {
  type Value = Number
  val dbType = "numeric"
  override def withNewColumnId(colId: IdType) = copy(columnId = Some(colId))
}

case class LinkColumn(table: Table, columnId: Option[IdType], to: IdType, name: String) extends ColumnType {
  type Value = Link
}

case class Link(value: Seq[IdType])

case class Table(id: IdType, name: String, columns: Seq[ColumnType]) {
  def getColumn(columnId: IdType)(implicit executionContext: ExecutionContext): Future[ColumnType] = {
    Future.apply(columns.find(_.columnId == columnId).get)
  }
}

object TableStructure {
  type IdType = Long
}

class TableStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def deinstall(): Future[Unit] = for {
    t <- transaction.beginTransaction()
    (t, res) <- t.query(s"""DROP SCHEMA public CASCADE""".stripMargin, Json.arr())
    (t, res) <- t.query(s"""CREATE SCHEMA public""".stripMargin, Json.arr())
    _ <- t.commit()
  } yield ()

  def setup(): Future[Unit] = for {
    t <- transaction.beginTransaction()
    (t, res) <- t.query(s"""
                     |CREATE TABLE system_table (
                     |  table_id BIGSERIAL,
                     |  user_table_names VARCHAR(255) NOT NULL,
                     |  PRIMARY KEY(table_id)
                     |)""".stripMargin,
      Json.arr())
    (t, res) <- t.query(s"""
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
      Json.arr())
    (t, res) <- t.query(s"""
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
      Json.arr())
    (t, res) <- t.query(s"""
                     |ALTER TABLE system_columns
                     |  ADD FOREIGN KEY(link_id)
                     |  REFERENCES system_link_table(link_id)
                     |  ON DELETE CASCADE""".stripMargin,
      Json.arr())
    _ <- t.commit()
  } yield ()

  def createTable(name: String): Future[IdType] = for {
    t <- transaction.beginTransaction()
    (t, result) <- t.query("INSERT INTO system_table (user_table_names) VALUES (?) RETURNING table_id", Json.arr(name))
    id <- Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr())
    (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr())
    _ <- t.commit()
  } yield id

  def getTable(tableId: IdType): Future[String] = for {
    t <- transaction.beginTransaction()
    (t, result) <- t.query("SELECT table_id, user_table_names FROM system_table WHERE table_id = ?", Json.arr(tableId))
    n <- Future.successful(result.getArray("results").get[JsonArray](0).get[String](1))
    _ <- t.commit()
  } yield n
}

class ColumnStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def insertColumn[T <: ValueColumnType](tableId: IdType, column: T): Future[Long] = for {
    t <- transaction.beginTransaction()
    (t, result) <- t.query(s"""
                     |INSERT INTO system_columns 
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id""".stripMargin,
      Json.arr(tableId, column.name))
    id <- Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_$id ${column.dbType})", Json.arr())
    _ <- t.commit()
  } yield id
}

class Tableaux(verticle: Verticle) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  val vertx = verticle.vertx
  val transaction = new Transaction(verticle)
  val tables = new TableStructure(transaction)
  val columns = new ColumnStructure(transaction)

  def create(name: String): Future[Table] = for {
    id <- tables.createTable(name)
  } yield new Table(id, name, List())

  def delete(id: IdType): Future[Unit] = ???

  def addColumn[T <: ValueColumnType](tableId: IdType, name: String, columnType: String): Future[T] = for {
    table <- getTable(tableId)
    column <- Future.successful {
      columnType match {
        case "text"    => StringColumn(table, None, name)
        case "numeric" => NumberColumn(table, None, name)
      }
    }
    id <- columns.insertColumn(table.id, column)
  } yield {
    column.withNewColumnId(id)
  }.asInstanceOf[T]

  def insertValue[T <: ColumnType](tableId: IdType, columnId: IdType, rowId: IdType, value: T): Future[Unit] = for {
    table <- getTable(tableId)
    column <- table.getColumn(columnId)
    v: column.Value <- Future.apply(value.asInstanceOf[column.Value])
    _ <- column.setValue(rowId, v)
  } yield ()

  def getTable(tableId: IdType): Future[Table] = for {
    name <- tables.getTable(tableId)
  } yield Table(tableId, name, List())

  def getColumn(columnId: IdType): Future[ColumnType] = ???

}
