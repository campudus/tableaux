package com.campudus.tableaux.database

import com.campudus.tableaux.database.Tableaux._
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.json.{ Json, JsonArray, JsonObject }
import org.vertx.scala.platform.Verticle
import scala.concurrent.Future

case class CreateColumn(name: String, kind: TableauxDbType, ordering: Option[Ordering], linkConnections: Option[LinkConnections])

sealed trait ReturnType

case object GetReturn extends ReturnType

case object SetReturn extends ReturnType

case object EmptyReturn extends ReturnType

sealed trait DomainObject {
  def getJson: JsonObject

  def setJson: JsonObject

  def emptyJson: JsonObject = Json.obj()

  def toJson(reType: ReturnType): JsonObject = reType match {
    case GetReturn => getJson
    case SetReturn => setJson
    case EmptyReturn => emptyJson
  }
}

sealed trait ColumnType[A] extends DomainObject {
  type Value = A

  def dbType: String

  def id: IdType

  def name: String

  def table: Table

  def ordering: Ordering

  def getJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "name" -> name, "kind" -> dbType, "ordering" -> ordering)))

  def setJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "ordering" -> ordering)))
}

sealed trait LinkColumnType[A] extends ColumnType[Link[A]] {
  def to: ColumnValue[A]

  override def getJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "name" -> name, "kind" -> dbType, "toTable" -> to.table.id, "toColumn" -> to.id, "ordering" -> ordering)))

  override def setJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "ordering" -> ordering)))
}

sealed trait ColumnValue[A] extends ColumnType[A]

case class StringColumn(table: Table, id: IdType, name: String, ordering: Ordering) extends ColumnValue[String] {
  val dbType = "text"
}

case class NumberColumn(table: Table, id: IdType, name: String, ordering: Ordering) extends ColumnValue[Number] {
  val dbType = "numeric"
}

case class LinkColumn[A](table: Table, id: IdType, to: ColumnValue[A], name: String, ordering: Ordering) extends LinkColumnType[A] {
  val dbType = "link"
}

case class ColumnSeq(columns: Seq[ColumnType[_]]) extends DomainObject {
  def getJson: JsonObject = Json.obj("tableId" -> columns(0).table.id, "columns" ->
    (columns map { col =>
      col match {
        case c: LinkColumnType[_] => Json.obj("id" -> c.id, "name" -> c.name, "kind" -> c.dbType, "toTable" -> c.to.table.id, "toColumn" -> c.to.id, "ordering" -> c.ordering)
        case c: ColumnValue[_] => Json.obj("id" -> c.id, "name" -> c.name, "kind" -> c.dbType, "ordering" -> c.ordering)
      }
    }))
  def setJson: JsonObject = Json.obj("columns" -> (columns map { col => Json.obj("id" -> col.id, "ordering" -> col.ordering) }))
}

sealed trait TableauxDbType

case object TextType extends TableauxDbType {
  override def toString(): String = "text"
}

case object NumericType extends TableauxDbType {
  override def toString(): String = "numeric"
}

case object LinkType extends TableauxDbType {
  override def toString(): String = "link"
}

object Mapper {
  def columnType(s: TableauxDbType): (Option[(Table, IdType, String, Ordering) => ColumnValue[_]], TableauxDbType) = s match {
    case TextType => (Some(StringColumn.apply), TextType)
    case NumericType => (Some(NumberColumn.apply), NumericType)
    case LinkType => (None, LinkType)
  }

  def getApply(s: TableauxDbType): (Table, IdType, String, Ordering) => ColumnValue[_] = columnType(s)._1.get

  def getDatabaseType(s: String): TableauxDbType = s match {
    case "text" => TextType
    case "numeric" => NumericType
    case "link" => LinkType
  }
}

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A) extends DomainObject {
  def getJson: JsonObject = {
    val v = value match {
      case link: Link[A] => link.toJson
      case _ => value
    }
    Json.obj("rows" -> Json.arr(Json.obj("value" -> v)))
  }

  def setJson: JsonObject = Json.obj()
}

case class Link[A](value: Seq[(IdType, A)]) {
  def toJson: Seq[JsonObject] = value map {
    case (id, v) => Json.obj("id" -> id, "value" -> v)
  }
}

case class Table(id: IdType, name: String) extends DomainObject {
  def getJson: JsonObject = Json.obj("tableId" -> id, "tableName" -> name)

  def setJson: JsonObject = Json.obj("tableId" -> id)
}

case class CompleteTable(table: Table, columnList: Seq[ColumnType[_]], rowList: RowSeq) extends DomainObject {
  def getJson: JsonObject = table.getJson.mergeIn(Json.obj("columns" -> (columnList map { _.getJson.getArray("columns").get[JsonObject](0) }))).mergeIn(rowList.getJson)

  def setJson: JsonObject = table.setJson.mergeIn(Json.obj("columns" -> (columnList map { (_.setJson.getArray("columns").get[JsonObject](0)) }))).mergeIn(rowList.setJson)
}

case class RowIdentifier(table: Table, id: IdType) extends DomainObject {
  def getJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))

  def setJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))
}

case class Row(table: Table, id: IdType, values: Seq[_]) extends DomainObject {
  def getJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id, "values" -> values)))

  def setJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))
}

case class RowSeq(rows: Seq[Row]) extends DomainObject {
  def getJson: JsonObject = Json.obj("rows" -> (rows map { r => Json.obj("id" -> r.id, "values" -> r.values) }))

  def setJson: JsonObject = Json.obj("rows" -> (rows map { r => Json.obj("id" -> r.id) }))
}

case class EmptyObject() extends DomainObject {
  def getJson: JsonObject = Json.obj()

  def setJson: JsonObject = Json.obj()
}

object Tableaux {
  type IdType = Long
  type Ordering = Long
  type LinkConnections = (IdType, IdType, IdType)
}

class Tableaux(verticle: Verticle) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)
  import scala.collection.JavaConverters._

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

  def createTable(name: String): Future[Table] = for {
    id <- tableStruc.create(name)
  } yield Table(id, name)

  def createCompleteTable(name: String, columnsNameAndType: Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = for {
    table <- createTable(name)
    columnIds <- addColumns(table.id, columnsNameAndType) map { colSeq => colSeq.columns map { col => col.id } }
    rowsWithColumnIdAndValue <- Future.successful {
      if (rowsValues.isEmpty) {
        Seq()
      } else {
        rowsValues map { columnIds.zip(_) }
      }
    }
    _ <- addFullRows(table.id, rowsWithColumnIdAndValue)
    completeTable <- getCompleteTable(table.id)
  } yield completeTable

  def deleteTable(id: IdType): Future[EmptyObject] = for {
    _ <- tableStruc.delete(id)
  } yield EmptyObject()

  def deleteRow(tableId: IdType, rowId: IdType): Future[EmptyObject] = for {
    _ <- rowStruc.delete(tableId, rowId)
  } yield EmptyObject()

  def addColumns(tableId: IdType, columns: Seq[CreateColumn]): Future[ColumnSeq] = for {
    cols <- Future.sequence { //FIXME random error due to race?
      columns map {
        case CreateColumn(name, LinkType, optOrd, Some((toTable, toColumn, fromColumn))) =>
          addLinkColumn(tableId, name, fromColumn, toTable, toColumn, optOrd)
        case CreateColumn(name, dbType, optOrd, optLink) =>
          addValueColumn(tableId, name, dbType, optOrd)
      }
    }
  } yield ColumnSeq(cols)

  def addValueColumn(tableId: IdType, name: String, columnType: TableauxDbType, ordering: Option[Ordering]): Future[ColumnValue[_]] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insert(table.id, columnType, name, ordering)
  } yield Mapper.getApply(columnType).apply(table, id, name, ordering)

  def addLinkColumn(tableId: IdType, name: String, fromColumn: IdType, toTable: IdType, toColumn: IdType, ordering: Option[Ordering]): Future[LinkColumnType[_]] = for {
    table <- getTable(tableId)
    toCol <- getColumn(toTable, toColumn).asInstanceOf[Future[ColumnValue[_]]]
    (id, ordering) <- columnStruc.insertLink(tableId, name, fromColumn, toCol.table.id, toCol.id, ordering)
  } yield LinkColumn(table, id, toCol, name, ordering)

  def removeColumn(tableId: IdType, columnId: IdType): Future[EmptyObject] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield EmptyObject()

  def addRow(tableId: IdType): Future[RowIdentifier] = for {
    table <- getTable(tableId)
    id <- rowStruc.create(tableId)
  } yield RowIdentifier(table, id)

  def addFullRows(tableId: IdType, values: Seq[Seq[(IdType, _)]]): Future[RowSeq] = for {
    table <- getTable(tableId)
    ids <- Future.sequence {
      for {
        tup <- values
      } yield rowStruc.createFull(table.id, tup) // FIXME random error due to race?
    }
    row <- Future.sequence(ids map { id => getRow(table.id, id) })
  } yield RowSeq(row)

  def insertValue[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[_, _]] = for {
    column <- getColumn(tableId, columnId)
    cell <- column match {
      case _: LinkColumnType[_] =>
        import scala.collection.JavaConverters._
        val valueList = value.asInstanceOf[JsonArray].asScala.toList.asInstanceOf[List[Number]]
        val valueFromList = (valueList(0).longValue(), valueList(1).longValue())
        insertLinkValue(tableId, columnId, rowId, valueFromList)
      case _ => insertNormalValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertNormalValue[A, B <: ColumnType[A]](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[A, B]] = for {
    column <- getColumn(tableId, columnId)
    _ <- cellStruc.update(tableId, columnId, rowId, value)
  } yield Cell(column.asInstanceOf[B], rowId, value)

  def insertLinkValue(tableId: IdType, columnId: IdType, rowId: IdType, value: (IdType, IdType)): Future[Cell[Link[Any], LinkColumnType[Any]]] = for {
    linkColumn <- getColumn(tableId, columnId).asInstanceOf[Future[LinkColumnType[Any]]]
    _ <- cellStruc.updateLink(linkColumn.table.id, linkColumn.id, value)
    v <- cellStruc.getLinkValues(linkColumn.table.id, linkColumn.id, rowId, linkColumn.to.table.id, linkColumn.to.id)
  } yield Cell(linkColumn, rowId, Link(List((value._2, v))))

  def getTable(tableId: IdType): Future[Table] = for {
    (id, name) <- tableStruc.get(tableId)
  } yield Table(id, name)

  def getRow(tableId: IdType, rowId: IdType): Future[Row] = for {
    table <- getTable(tableId)
    (id, seqOfValues) <- rowStruc.get(tableId, rowId)
  } yield Row(table, id, seqOfValues)

  def getCell(tableId: IdType, columnId: IdType, rowId: IdType): Future[Cell[_, _]] = for {
    column <- getColumn(tableId, columnId)
    value <- column match {
      case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)
      case _ => cellStruc.getValue(tableId, columnId, rowId)
    }
  } yield Cell(column.asInstanceOf[ColumnType[Any]], rowId, value)

  def getColumn(tableId: IdType, columnId: IdType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (columnId, columnName, columnKind, ordering) <- columnStruc.get(table.id, columnId)
    column <- columnKind match {
      case LinkType => getLinkColumn(table, columnId, columnName, ordering)
      case kind => Future.successful(getValueColumn(table, columnId, columnName, kind, ordering))
    }
  } yield column

  private def getValueColumn(table: Table, columnId: IdType, columnName: String, columnKind: TableauxDbType, ordering: Ordering): ColumnValue[_] = {
    Mapper.getApply(columnKind).apply(table, columnId, columnName, ordering)
  }

  private def getLinkColumn(table: Table, columnId: IdType, columnName: String, ordering: Ordering): Future[LinkColumnType[_]] = for {
    (tableId, toColumnId) <- columnStruc.getToColumn(table.id, columnId)
    toCol <- getColumn(tableId, toColumnId).asInstanceOf[Future[ColumnValue[_]]]
  } yield LinkColumn(table, columnId, toCol, columnName, ordering)

  def getCompleteTable(tableId: IdType): Future[CompleteTable] = for {
    table <- getTable(tableId)
    colList <- getAllColumns(table)
    rowList <- getAllRows(table)
  } yield CompleteTable(table, colList, rowList)

  private def getAllColumns(table: Table): Future[Seq[ColumnType[_]]] = for {
    allColumns <- columnStruc.getAll(table.id)
  } yield allColumns map {
    case (columnId, columnName, columnKind, ordering) =>
      Mapper.getApply(columnKind).apply(table, columnId, columnName, ordering)
  }

  private def getAllRows(table: Table): Future[RowSeq] = for {
    allRows <- rowStruc.getAll(table.id)
  } yield RowSeq(allRows map { case (rowId, values) => Row(table, rowId, values) })

  def changeTableName(tableId: IdType, tableName: String): Future[Table] = for {
    _ <- tableStruc.changeName(tableId, tableName)
  } yield Table(tableId, tableName)

  def changeColumn(tableId: IdType, columnId: IdType, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[ColumnType[_]] = for {
    _ <- columnStruc.change(tableId, columnId, columnName, ordering, kind)
    column <- getColumn(tableId, columnId)
  } yield column
}
