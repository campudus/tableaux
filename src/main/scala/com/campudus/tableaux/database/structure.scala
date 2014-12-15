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

sealed trait ColumnType[A] {
  type Value = A

  def dbType: String
  def id: IdType
  def name: String
  def table: Table
}

case class StringColumn(table: Table, id: IdType, name: String) extends ColumnType[String] {
  val dbType = "text"
}
case class NumberColumn(table: Table, id: IdType, name: String) extends ColumnType[Number] {
  val dbType = "numeric"
}

case class LinkColumn(table: Table, id: IdType, to: IdType, name: String) extends ColumnType[Link] {
  val dbType = "Link"
}

object Mapper {
  def ctype(s: String): ((Table, IdType, String) => ColumnType[_], String) = s match {
    case "text" => (StringColumn.apply _, "text")
    case "numeric" => (NumberColumn.apply _, "numeric")
  }

  def getApply(s: String): (Table, IdType, String) => ColumnType[_] = ctype(s)._1
  def getDatabaseType(s: String): String = ctype(s)._2
}

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A)

case class Link(value: Seq[IdType])

case class Table(id: IdType, name: String)

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
                     |  column_type VARCHAR(255) NOT NULL,
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

  def insert(tableId: IdType, dbType: String, name: String): Future[IdType] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"""
                     |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering)
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id""".stripMargin,
      Json.arr(tableId, dbType, name))
    id <- Future.successful(result.getArray("results").get[JsonArray](0).get[Long](0))
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_$id $dbType", Json.arr())
    _ <- t.commit()
  } yield id

  def get(table: Table, columnId: IdType): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query("""
                     |SELECT column_id, user_column_name, column_type
                     |  FROM system_columns 
                     |  WHERE table_id = ? AND column_id = ? 
                     |  ORDER BY column_id""".stripMargin, Json.arr(table.id, columnId))
    j <- Future.successful(result.getArray("results").get[JsonArray](0))
    _ <- t.commit()
  } yield j

  def getAll(table: Table): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query("""
                     |SELECT column_id, user_column_name, column_type
                     |  FROM system_columns 
                     |  WHERE table_id = ? ORDER BY column_id""".stripMargin, Json.arr(table.id))
    j <- Future.successful(result.getArray("results"))
    _ <- t.commit()
  } yield j

  def delete(tableId: IdType, columnId: IdType): Future[Unit] = for {
    t <- transaction.begin()
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId", Json.arr())
    (t, _) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
    _ <- t.commit()
  } yield ()

  def typeMatcher(table: Table, json: JsonArray): Future[List[ColumnType[_]]] = for {
    x <- Future.successful {
      import collection.JavaConverters._
      var l: List[ColumnType[_]] = List()
//      val liste = t.getArray("results").toList().asScala.asInstanceOf[List[JsonArray]].map(_.get[String](1)).foldLeft(List()) { (s, e) => s ::: List() }
      for (j <- json.toList().asScala.asInstanceOf[List[JsonArray]]) {
        json.get[String](2) match {
          case "text"    => l = l ::: List(StringColumn(table, j.get[IdType](0), j.get[String](1)))
          case "numeric" => l = l ::: List(NumberColumn(table, j.get[IdType](0), j.get[String](1)))
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

  def update[A, B <: ColumnType[A]](cell: Cell[A, B]): Future[Unit] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"UPDATE user_table_${cell.column.table.id} SET column_${cell.column.id} = ? WHERE id = ?", Json.arr(cell.value, cell.rowId))
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
  } yield Table(id, name)

  def delete(id: IdType): Future[Unit] = for {
    _ <- tableStruc.delete(id)
  } yield ()

  def addColumn(tableId: IdType, name: String, columnType: String): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (colApply, dbType) <- Future.successful {
      Mapper.ctype(columnType)
    }
    id <- columnStruc.insert(table.id, dbType, name)
  } yield {
    colApply(table, id, name)
  }

  def removeColumn(tableId: IdType, columnId: IdType): Future[Unit] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield ()

  def addRow(tableId: IdType): Future[IdType] = for {
    id <- rowStruc.create(tableId)
  } yield id

  def insertValue[A, B <: ColumnType[A]](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[A, B]] = for {
    table <- getTable(tableId)
    column <- getColumn(tableId, columnId)
    v <- Future.apply(value.asInstanceOf[column.Value])
    cell <- Future.successful { Cell[A, B](column.asInstanceOf[B], rowId, v.asInstanceOf[A]) }
    _ <- cellStruc.update[A, B](cell)
  } yield cell

  def getTable(tableId: IdType): Future[Table] = for {
    json <- tableStruc.get(tableId)
  } yield Table(json.get[Long](0), json.get[String](1))

  def getColumn(tableId: IdType, columnId: IdType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    result <- columnStruc.get(table, columnId)
    column <- Future.successful[ColumnType[_]] {
      Mapper.getApply(result.get[String](2)).apply(table, result.get[IdType](0), result.get[String](1))
    }
  } yield column

  def getCompleteTable(tableId: IdType): Future[(Table, Seq[(ColumnType[_], Seq[Cell[_,_]])])] = ???
  
  def getAllColumns(table: Table): Future[Seq[ColumnType[_]]] = ???
  
  def getAllCells(column: ColumnType[_]): Future[Seq[Cell[_, _]]] = ???
  
  def getAllTableCells(table: Table): Future[Seq[(ColumnType[_], Seq[Cell[_, _]])]] = ???

  /* getTable
    json <- columnStruc.getAll(table)
    columns <- columnStruc.typeMatcher(table, json) 
   */

}
