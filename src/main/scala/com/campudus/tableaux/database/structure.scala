package com.campudus.tableaux.database

import com.campudus.tableaux.database.TableStructure._
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.json.{ Json, JsonArray }
import org.vertx.scala.platform.Verticle

import scala.concurrent.Future

sealed trait ColumnType[A] {
  type Value = A

  def dbType: String

  def id: IdType

  def name: String

  def table: Table
}

sealed trait LinkType extends ColumnType[Link] { 
  def to: ColumnType[_]
}

case class StringColumn(table: Table, id: IdType, name: String) extends ColumnType[String] {
  val dbType = "text"
}

case class NumberColumn(table: Table, id: IdType, name: String) extends ColumnType[Number] {
  val dbType = "numeric"
}

case class LinkColumn(table: Table, id: IdType, to: ColumnType[_], name: String) extends LinkType {
  val dbType = "link"
}

object Mapper {
  def ctype(s: String): ((Table, IdType, String) => ColumnType[_], String) = s match {
    case "text"    => (StringColumn.apply, "text")
    case "numeric" => (NumberColumn.apply, "numeric")
    case "link"    => (NumberColumn.apply, "link")
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

class SystemStructure(transaction: DatabaseConnection) {
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

class TableStructure(transaction: DatabaseConnection) {
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

class ColumnStructure(transaction: DatabaseConnection) {
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

  def insertLink(tableId: IdType, name: String, fromColumn: IdType, toColumn: ColumnType[_]): Future[IdType] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"""INSERT INTO system_link_table (table_id_1, table_id_2, column_id_1, column_id_2) VALUES (?, ?, ?, ?) RETURNING link_id""".stripMargin,
      Json.arr(tableId, toColumn.table.id, fromColumn, toColumn.id))
    linkId <- Future.successful { result.getArray("results").get[JsonArray](0).get[Long](0) }
    (t, _) <- t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?, 
                    |  nextval('system_columns_column_id_table_${toColumn.table.id}'), 
                    |  'link',
                    |  ?, 
                    |  currval('system_columns_column_id_table_${toColumn.table.id}'), 
                    |  ?)""".stripMargin,
      Json.arr(toColumn.table.id, name, linkId))
    (t, result) <- t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?, 
                    |  nextval('system_columns_column_id_table_$tableId'), 
                    |  'link',
                    |  ?, 
                    |  currval('system_columns_column_id_table_$tableId'), 
                    |  ?
                    |) RETURNING column_id""".stripMargin,
      Json.arr(tableId, name, linkId))
    (t, _) <- t.query(s"""
                    |CREATE TABLE link_table_$linkId (
                    |  id_1 bigint, 
                    |  id_2 bigint, 
                    |  PRIMARY KEY(id_1, id_2), 
                    |  CONSTRAINT link_table_${linkId}_foreign_1 
                    |    FOREIGN KEY(id_1) 
                    |    REFERENCES user_table_$tableId (id) 
                    |    ON DELETE CASCADE, 
                    |  CONSTRAINT link_table_${linkId}_foreign_2
                    |    FOREIGN KEY(id_2) 
                    |    REFERENCES user_table_${toColumn.table.id} (id) 
                    |    ON DELETE CASCADE
                    |)""".stripMargin, Json.arr())
    id <- Future.successful { result.getArray("results").get[JsonArray](0).get[Long](0) }
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
    j <- Future.successful {
      result.getArray("results")
    }
    _ <- t.commit()
  } yield j

  def delete(tableId: IdType, columnId: IdType): Future[Unit] = for {
    t <- transaction.begin()
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId", Json.arr())
    (t, _) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
    _ <- t.commit()
  } yield ()

}

class RowStructure(transaction: DatabaseConnection) {
  implicit val executionContext = transaction.executionContext

  def create(tableId: IdType): Future[IdType] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id", Json.arr())
    j <- Future.successful {
      result.getArray("results").get[JsonArray](0).get[IdType](0)
    }
    _ <- t.commit()
  } yield j

  def getAllFromColumn(column: ColumnType[_]): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"SELECT id, column_${column.id} FROM user_table_${column.table.id} ORDER BY id", Json.arr())
    j <- Future.successful {
      result.getArray("results")
    }
    _ <- t.commit()
  } yield j

  def getAll(column: ColumnType[_]): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query(s"SELECT * FROM user_table_${column.table.id} ORDER BY id", Json.arr())
    j <- Future.successful {
      result.getArray("results")
    }
    _ <- t.commit()
  } yield j
}

class CellStructure(transaction: DatabaseConnection) {
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
  val dbConnection = new DatabaseConnection(verticle)
  val tableStruc = new TableStructure(dbConnection)
  val columnStruc = new ColumnStructure(dbConnection)
  val cellStruc = new CellStructure(dbConnection)
  val rowStruc = new RowStructure(dbConnection)

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
  } yield colApply(table, id, name)

  def addLinkColumn(tableId: IdType, name: String, fromColumn: IdType, toTable: IdType, toColumn: IdType): Future[LinkType] = for {
    table <- getTable(tableId)
    toCol <- getColumn(toTable, toColumn)
    id <- columnStruc.insertLink(tableId, name, fromColumn, toCol)
  } yield LinkColumn(table, id, toCol, name)

  def removeColumn(tableId: IdType, columnId: IdType): Future[Unit] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield ()

  def addRow(tableId: IdType): Future[IdType] = for {
    id <- rowStruc.create(tableId)
  } yield id

  def insertValue[A, B <: ColumnType[A]](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[A, B]] = for {
    table <- getTable(tableId)
    column <- getColumn(tableId, columnId)
    value <- Future.apply(value.asInstanceOf[column.Value])
    cell <- Future.successful {
      Cell[A, B](column.asInstanceOf[B], rowId, value.asInstanceOf[A])
    }
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

  def getCompleteTable(tableId: IdType): Future[(Table, Seq[(ColumnType[_], Seq[Cell[_, _]])])] = for {
    table <- getTable(tableId)
    cc <- getAllTableCells(table)
  } yield (table, cc)

  private def getAllColumns(table: Table): Future[Seq[ColumnType[_]]] = {
    for {
      results <- columnStruc.getAll(table)
    } yield {
      val listOfList = resultsInListOfList(results)
      listOfList map { jsonRes =>
        Mapper.getApply(jsonRes.get(2)).apply(table, jsonRes.get(0), jsonRes.get(1))
      }
    }
  }

  private def getAllRowsFromColumn[A](column: ColumnType[A]): Future[Seq[Cell[A, ColumnType[A]]]] = {
    for {
      results <- rowStruc.getAllFromColumn(column)
    } yield {
      val listOfLists = resultsInListOfList(results)
      listOfLists map { jsonRes =>
        Cell[A, ColumnType[A]](column, jsonRes.get(0), jsonRes.get(1))
      }
    }
  }

  private def getAllTableCells(table: Table): Future[Seq[(ColumnType[_], Seq[Cell[_, _]])]] = {
    getAllColumns(table) flatMap { seqColumn => Future.sequence(seqColumn map { column => getAllRowsFromColumn(column) map { seqCell => (column, seqCell) } }) }
  }

  private def resultsInListOfList(results: JsonArray): Seq[JsonArray] = {
    import scala.collection.JavaConverters._
    val listOfJsonArray = (for {
      elem <- results.iterator().asScala
    } yield {
      elem.asInstanceOf[JsonArray]
    }).toStream
    listOfJsonArray
  }

}
