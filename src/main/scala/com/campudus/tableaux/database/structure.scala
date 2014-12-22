package com.campudus.tableaux.database

import com.campudus.tableaux.database.TableStructure._
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.json.{ Json, JsonArray }
import org.vertx.scala.platform.Verticle
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure

sealed trait ColumnType[A] {
  type Value = A

  def dbType: String

  def id: IdType

  def name: String

  def table: Table
}

sealed trait LinkType[A] extends ColumnType[Link[A]] {
  def to: ColumnValue[A]
}

sealed trait ColumnValue[A] extends ColumnType[A]

case class StringColumn(table: Table, id: IdType, name: String) extends ColumnValue[String] {
  val dbType = "text"
}

case class NumberColumn(table: Table, id: IdType, name: String) extends ColumnValue[Number] {
  val dbType = "numeric"
}

case class LinkColumn[A](table: Table, id: IdType, to: ColumnValue[A], name: String) extends LinkType[A] {
  val dbType = "link"
}

object Mapper {
  def ctype(s: String): (Option[(Table, IdType, String) => ColumnValue[_]], String) = s match {
    case "text"    => (Some(StringColumn.apply), "text")
    case "numeric" => (Some(NumberColumn.apply), "numeric")
    case "link"    => (None, "link")
  }

  def getApply(s: String): (Table, IdType, String) => ColumnValue[_] = ctype(s)._1.get

  def getDatabaseType(s: String): String = ctype(s)._2
}

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A)

case class Link[A](value: Seq[(IdType, A)])

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

  def getToColumn(tableId: IdType, columnId: IdType): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query("""
                              |SELECT table_id, column_id
                              |  FROM system_columns
                              |  WHERE table_id != ? AND column_id != ? 
                              |  ORDER BY column_id""".stripMargin, Json.arr(tableId, columnId))
    j <- Future.successful {
      result.getArray("results").get[JsonArray](0)
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

  def updateLink(column: ColumnType[_], values: (IdType, IdType)): Future[Unit] = for {
    t <- transaction.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(column.table.id, column.id))
    linkId <- Future.successful {
      result.getArray("results").get[JsonArray](0).get[IdType](0)
    }
    (t, _) <- t.query(s"INSERT INTO link_table_$linkId VALUES (?, ?)", Json.arr(values._1, values._2))
    _ <- t.commit()
  } yield ()

  def getLinkValues(column: LinkType[_], rowId: IdType): Future[JsonArray] = for {
    t <- transaction.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(column.table.id, column.id))
    linkId <- Future.successful(result.getArray("results").get[JsonArray](0).get[IdType](0))
    (t, result) <- t.query("SELECT table_id_1, table_id_2, column_id_1, column_id_2 FROM system_link_table WHERE link_id = ?", Json.arr(linkId))
    (id1, id2) <- Future.successful {
      val res = result.getArray("results").get[JsonArray](0)
      val linkTo2 = (res.get[IdType](1), res.get[IdType](3))

      if (linkTo2 == (column.to.table.id, column.to.id)) ("id_1", "id_2") else ("id_2", "id_1")
    }
    (t, result) <- t.query(s"""
                     |SELECT STRING_AGG(user_table_${column.to.table.id}.column_${column.to.id}, ', ') FROM user_table_${column.table.id} 
                     |  LEFT JOIN link_table_$linkId 
                     |    ON user_table_${column.table.id}.id = link_table_${linkId}.$id1
                     |  LEFT JOIN user_table_${column.to.table.id} 
                     |    ON user_table_${column.to.table.id}.id = link_table_${linkId}.$id2
                     |  WHERE user_table_${column.table.id}.id = ?
                     |  GROUP BY user_table_${column.table.id}.id""".stripMargin, Json.arr(rowId))
    j <- Future.successful(result.getArray("results"))
    _ <- t.commit()
  } yield j
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

  def addColumn(tableId: IdType, name: String, columnType: String): Future[ColumnValue[_]] = for {
    table <- getTable(tableId)
    (colApply, dbType) <- Future.successful {
      Mapper.ctype(columnType)
    }
    id <- columnStruc.insert(table.id, dbType, name)
  } yield colApply.get.apply(table, id, name)

  def addLinkColumn(tableId: IdType, name: String, fromColumn: IdType, toTable: IdType, toColumn: IdType): Future[LinkType[_]] = for {
    table <- getTable(tableId)
    toCol <- getColumn(toTable, toColumn).asInstanceOf[Future[ColumnValue[_]]]
    id <- columnStruc.insertLink(tableId, name, fromColumn, toCol)
  } yield LinkColumn(table, id, toCol, name)

  def removeColumn(tableId: IdType, columnId: IdType): Future[Unit] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield ()

  def addRow(tableId: IdType): Future[IdType] = for {
    id <- rowStruc.create(tableId)
  } yield id

  def insertValue[A, B <: ColumnType[A]](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[A, B]] = for {
    column <- getColumn(tableId, columnId)
    cell <- Future.successful {
      Cell[A, B](column.asInstanceOf[B], rowId, value.asInstanceOf[A])
    }
    _ <- cellStruc.update[A, B](cell)
  } yield cell

  def insertLinkValue(tableId: IdType, columnId: IdType, rowId: IdType, value: (IdType, IdType)): Future[Cell[Link[_], ColumnType[Link[_]]]] = for {
    linkColumn <- getColumn(tableId, columnId).asInstanceOf[Future[LinkType[_]]]
    _ <- cellStruc.updateLink(linkColumn, value)
    jr <- cellStruc.getLinkValues(linkColumn, rowId)
    v <- Future.successful {
      jr.get[JsonArray](0).get[String](0)
    }
    cell <- Future.successful(Cell[Link[v.type], LinkType[v.type]](linkColumn.asInstanceOf[LinkType[v.type]], rowId, Link(List((value._2, v)))))
  } yield cell.asInstanceOf[Cell[Link[_], ColumnType[Link[_]]]]

  def getTable(tableId: IdType): Future[Table] = for {
    json <- tableStruc.get(tableId)
  } yield Table(json.get[Long](0), json.get[String](1))
  
  def getColumn(tableId: IdType, columnId: IdType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    result <- columnStruc.get(table, columnId)
    column <- Mapper.getDatabaseType(result.get[String](2)) match {
      case "link" => getLinkColumn(table, result)
      case _ => Future.successful(getValueColumn(table, result))
    }
  } yield column.asInstanceOf[ColumnType[_]]

  private def getValueColumn(table: Table, result: JsonArray): ColumnValue[_] = Mapper.getApply(result.get[String](2)).apply(table, result.get[IdType](0), result.get[String](1))
  
  private def getLinkColumn(table: Table, result: JsonArray): Future[LinkType[_]] = for {
    (columnId, columnName) <- Future.successful {
      (result.get[IdType](0), result.get[String](1))
    }
    jsonResult <- columnStruc.getToColumn(table.id, columnId)
    toCol <- getColumn(jsonResult.get[Long](0), jsonResult.get[Long](1)).asInstanceOf[Future[ColumnValue[_]]]
  } yield LinkColumn(table, columnId, toCol, columnName)

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
