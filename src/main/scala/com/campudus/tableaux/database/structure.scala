package com.campudus.tableaux.database

import com.campudus.tableaux.database.TableStructure._
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.json.{ Json, JsonArray, JsonObject }
import org.vertx.scala.platform.Verticle
import scala.concurrent.Future

sealed trait DomainObject {
  def toJson: JsonObject
}

sealed trait ColumnType[A] extends DomainObject {
  type Value = A

  def dbType: String

  def id: IdType

  def name: String

  def table: Table

  def toJson: JsonObject = Json.obj("tableId" -> table.id, "columnId" -> id, "columnName" -> name, "type" -> dbType)
}

sealed trait LinkType[A] extends ColumnType[Link[A]] {
  def to: ColumnValue[A]

  override def toJson: JsonObject = super.toJson.mergeIn(Json.obj("toTable" -> to.table.id, "toColumn" -> to.id))
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

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A) extends DomainObject {
  def toJson: JsonObject = {
    val v = value match {
      case link: Link[A] => link.toJson
      case _             => value
    }
    Json.obj("tableId" -> column.table.id, "columnId" -> column.id, "rowId" -> rowId, "value" -> v)
  }
}

case class Link[A](value: Seq[(IdType, A)]) {
  def toJson: Seq[JsonObject] = value map {
    case (id, v) => Json.obj("id" -> id, "value" -> v)
  }
}

case class Table(id: IdType, name: String) extends DomainObject {
  def toJson: JsonObject = Json.obj("tableId" -> id, "tableName" -> name)
}

case class CompleteTable(table: Table, columnList: Seq[(ColumnType[_], Seq[Cell[_, _]])]) extends DomainObject {
  def toJson: JsonObject = {
    val columnsJson = columnList map { case (col, _) => Json.obj("id" -> col.id, "name" -> col.name) }
    val fromColumnValueToRowValue = columnList flatMap { case (col, colValues) => colValues map { cell => Json.obj("id" -> cell.rowId, s"c${col.id}" -> cell.value) } }
    val rowsJson = fromColumnValueToRowValue.foldLeft(Seq[JsonObject]()) { (finalRowValues, rowValues) =>
      val helper = fromColumnValueToRowValue.filter { filterJs => filterJs.getLong("id") == rowValues.getLong("id") }.foldLeft(Json.obj()) { (js, filteredJs) => js.mergeIn(filteredJs) }
      if (finalRowValues.contains(helper)) finalRowValues else finalRowValues :+ helper
    }

    table.toJson.mergeIn(Json.obj("cols" -> columnsJson, "rows" -> rowsJson))
  }
}

case class Row(table: Table, id: IdType) extends DomainObject {
  def toJson: JsonObject = Json.obj("tableId" -> table.id, "rowId" -> id)
}

case class EmptyObject() extends DomainObject {
  def toJson: JsonObject = Json.obj()
}

class Tableaux(verticle: Verticle) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)

  val vertx = verticle.vertx
  val dbConnection = new DatabaseConnection(verticle)
  val systemStruc = new SystemStructure(dbConnection)
  val tableStruc = new TableStructure(dbConnection)
  val columnStruc = new ColumnStructure(dbConnection)
  val cellStruc = new CellStructure(dbConnection)
  val rowStruc = new RowStructure(dbConnection)

  def resetDB(): Future[EmptyObject] = for {
    _ <- systemStruc.deinstall()
    _ <- systemStruc.setup()
  } yield EmptyObject()

  def create(name: String): Future[Table] = for {
    id <- tableStruc.create(name)
  } yield Table(id, name)

  def delete(id: IdType): Future[EmptyObject] = for {
    _ <- tableStruc.delete(id)
  } yield EmptyObject()

  def deleteRow(tableId: IdType, rowId: IdType): Future[EmptyObject] = for {
    _ <- rowStruc.delete(tableId, rowId)
  } yield EmptyObject()

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

  def removeColumn(tableId: IdType, columnId: IdType): Future[EmptyObject] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield EmptyObject()

  def addRow(tableId: IdType): Future[Row] = for {
    table <- getTable(tableId)
    id <- rowStruc.create(tableId)
  } yield Row(table, id)

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
      case _      => Future.successful(getValueColumn(table, result))
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

  def getCompleteTable(tableId: IdType): Future[CompleteTable] = for {
    table <- getTable(tableId)
    cc <- getAllTableCells(table)
  } yield CompleteTable(table, cc)

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
