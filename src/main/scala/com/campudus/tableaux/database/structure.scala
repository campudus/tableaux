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

sealed trait ColumnType {
  type Value

  def columnId: Option[IdType]

  def name: String

  def cells: Seq[Cell[Value]]

  def table: Table

  def setValue(rowId: IdType, value: Value): ColumnType

  def getCell(rowId: IdType)(implicit executionContext: ExecutionContext): Future[Cell[Value]] = {
    Future.apply(cells.find(_.rowId == rowId).get)
  }

}

sealed trait ValueColumnType extends ColumnType {
  def dbType: String
  def withNewColumnId(colId: IdType): ValueColumnType
}

case class StringColumn(table: Table, columnId: Option[IdType], name: String, cells: Seq[Cell[String]]) extends ValueColumnType {
  type Value = String
  val dbType = "text"
  override def withNewColumnId(colId: IdType) = copy(columnId = Some(colId))
  override def setValue(rowId: IdType, value: Value) = copy(cells = cells :+ Cell[Value](this, rowId, value))
}

case class NumberColumn(table: Table, columnId: Option[IdType], name: String, cells: Seq[Cell[Number]]) extends ValueColumnType {
  type Value = Number
  val dbType = "numeric"
  override def withNewColumnId(colId: IdType) = copy(columnId = Some(colId))
  override def setValue(rowId: IdType, value: Value) = copy(cells = cells :+ Cell[Value](this, rowId, value))
}

case class LinkColumn(table: Table, columnId: Option[IdType], to: IdType, name: String, cells: Seq[Cell[Link]]) extends ColumnType {
  type Value = Link
  
  override def setValue(rowId: IdType, value: Value) = copy(cells = cells :+ Cell[Value](this, rowId, value))
}

case class Cell[+T](column: ColumnType, rowId: IdType, value: T) {
}

case class Link(value: Seq[IdType])

case class Table(id: IdType, name: String, columns: Seq[ColumnType]) {
  def withNewColumnSeq(cols: Seq[ColumnType]) = copy(columns = cols)

  def getColumn(columnId: IdType)(implicit executionContext: ExecutionContext): Future[ColumnType] = {
    Future.apply(columns.find(_.columnId.get == columnId).get)
  }
}

object TableStructure {
  type IdType = Long
}

class SystemStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def deinstall(): Future[Unit] = for {
    t <- transaction.begin()
    (t, res) <- t.query(s"""DROP SCHEMA public CASCADE""".stripMargin, Json.arr())
    (t, res) <- t.query(s"""CREATE SCHEMA public""".stripMargin, Json.arr())
    _ <- t.commit()
  } yield ()

  def setup(): Future[Unit] = for {
    t <- transaction.begin()
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
}

class TableStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def create(name: String): Future[IdType] = for {
    t <- transaction.begin()
    (t, result) <- t.query("INSERT INTO system_table (user_table_names) VALUES (?) RETURNING table_id", Json.arr(name))
    id <- Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))", Json.arr())
    (t, _) <- t.query(s"CREATE SEQUENCE system_columns_column_id_table_$id", Json.arr())
    _ <- t.commit()
  } yield id

  def get(tableId: IdType): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query("SELECT table_id, user_table_names FROM system_table WHERE table_id = ?", Json.arr(tableId))
    n <- Future.successful(result.getArray("results").get[JsonArray](0))
    _ <- t.commit()
  } yield n

  def delete(tableId: IdType): Future[Unit] = for {
    t <- transaction.begin()
    (t, _) <- t.query(s"DROP TABLE user_table_$tableId", Json.arr())
    (t, _) <- t.query("DELETE FROM system_table WHERE table_id = ?", Json.arr(tableId))
    (t, _) <- t.query(s"DROP SEQUENCE IF EXISTS system_columns_column_id_table_$tableId", Json.arr())
    _ <- t.commit()
  } yield ()
}

class ColumnStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def insert[T <: ValueColumnType](tableId: IdType, column: T): Future[IdType] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"""
                     |INSERT INTO system_columns 
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id""".stripMargin,
      Json.arr(tableId, column.name))
    id <- Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_$id ${column.dbType}", Json.arr())
    _ <- t.commit()
  } yield id

  def get(tableId: IdType): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query("""
                     |SELECT column_id, user_column_name 
                     |  FROM system_columns 
                     |  WHERE table_id = ? ORDER BY column_id""".stripMargin, Json.arr(tableId))
    j <- Future.successful(result.getArray("results"))
    _ <- t.commit()
  } yield j

  def getType(tableId: IdType): Future[JsonObject] = for {
    t <- transaction.begin()
    (t, result) <- t.query("SELECT column_name, data_type FROM information_schema.columns WHERE column_name LIKE 'column_%' AND table_name = ?", Json.arr(s"user_table_$tableId"))
    j <- Future.successful(result)
    _ <- t.commit()
  } yield j

  def delete(tableId: IdType, columnId: IdType): Future[Unit] = for {
    t <- transaction.begin()
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId", Json.arr())
    (t, _) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
    _ <- t.commit()
  } yield ()

  def typeMatcher(table: Table, json: JsonArray): Future[List[ColumnType]] = for {
    t <- getType(table.id)
    x <- Future.successful {
      var l: List[ColumnType] = List()
      for (i <- 0 until t.getInteger("rows")) {
        t.getArray("results").get[JsonArray](i).get[String](1) match {
          case "text"    => l = l ::: List(StringColumn(table, Some(json.get[JsonArray](i).get[IdType](0)), json.get[JsonArray](i).get[String](1), List()))
          case "numeric" => l = l ::: List(NumberColumn(table, Some(json.get[JsonArray](i).get[IdType](0)), json.get[JsonArray](i).get[String](1), List()))
        }
      }
      l
    }
  } yield x
}

class RowStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def create(tableId: IdType): Future[IdType] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id", Json.arr())
    j <- Future.successful { result.getArray("results").get[JsonArray](0).get[IdType](0) }
    _ <- t.commit()
  } yield j
}

class CellStructure(transaction: Transaction) {
  implicit val executionContext = transaction.executionContext

  def update[T](tableId: IdType, columnId: IdType, rowId: IdType, value: T): Future[Unit] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"UPDATE user_table_$tableId SET column_$columnId = ? WHERE id = ?", Json.arr(value, rowId))
    _ <- t.commit()
  } yield ()

}

class Tableaux(verticle: Verticle) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  val vertx = verticle.vertx
  val transaction = new Transaction(verticle)
  val tableStruc = new TableStructure(transaction)
  val columnStruc = new ColumnStructure(transaction)
  val cellStruc = new CellStructure(transaction)
  val rowStruc = new RowStructure(transaction)

  def create(name: String): Future[Table] = for {
    id <- tableStruc.create(name)
  } yield new Table(id, name, List())

  def delete(id: IdType): Future[Unit] = for {
    _ <- tableStruc.delete(id)
  } yield ()

  def addColumn[T <: ValueColumnType](tableId: IdType, name: String, columnType: String): Future[T] = for {
    table <- getTable(tableId)
    column <- Future.successful {
      columnType match {
        case "text"    => StringColumn(table, None, name, List())
        case "numeric" => NumberColumn(table, None, name, List())
      }
    }
    id <- columnStruc.insert(table.id, column)
  } yield {
    column.withNewColumnId(id)
  }.asInstanceOf[T]

  def removeColumn(tableId: IdType, columnId: IdType): Future[Unit] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield ()

  def addRow(tableId: IdType): Future[IdType] = for {
    id <- rowStruc.create(tableId)
  } yield id

  def insertValue[T <: ColumnType, U <: Any](tableId: IdType, columnId: IdType, rowId: IdType, value: U): Future[Cell[U]] = for {
    table <- getTable(tableId)
    column <- table.getColumn(columnId)
    v <- Future.apply(value.asInstanceOf[column.Value])
    _ <- cellStruc.update[column.Value](tableId, columnId, rowId, v)
    column <- Future.successful { column.setValue(rowId, v) }
    cell <- column.getCell(rowId)
  } yield cell.asInstanceOf[Cell[U]]

  def getTable(tableId: IdType): Future[Table] = for {
    json <- tableStruc.get(tableId)
    table <- Future.successful { Table(json.get[Long](0), json.get[String](1), List()) }
    json <- columnStruc.get(table.id)
    columns <- columnStruc.typeMatcher(table, json)
  } yield table.withNewColumnSeq(columns)

  def getColumn(tableId: IdType, columnId: IdType): Future[ColumnType] = for {
    table <- getTable(tableId)
    column <- table.getColumn(columnId)
  } yield column

}
