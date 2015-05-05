package com.campudus.tableaux.database

import com.campudus.tableaux.database.Tableaux._
import org.vertx.scala.core.json.{Json, JsonObject}

case class CreateColumn(name: String, kind: TableauxDbType, ordering: Option[Ordering], linkConnections: Option[LinkConnection])

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
  def getJson: JsonObject = Json.obj("tableId" -> columns.head.table.id, "columns" ->
    (columns map {
      case c: LinkColumnType[_] => Json.obj("id" -> c.id, "name" -> c.name, "kind" -> c.dbType, "toTable" -> c.to.table.id, "toColumn" -> c.to.id, "ordering" -> c.ordering)
      case c: ColumnValue[_] => Json.obj("id" -> c.id, "name" -> c.name, "kind" -> c.dbType, "ordering" -> c.ordering)
    }))
  def setJson: JsonObject = Json.obj("columns" -> (columns map { col => Json.obj("id" -> col.id, "ordering" -> col.ordering) }))
}

sealed trait TableauxDbType

case object TextType extends TableauxDbType {
  override def toString: String = "text"
}

case object NumericType extends TableauxDbType {
  override def toString: String = "numeric"
}

case object LinkType extends TableauxDbType {
  override def toString: String = "link"
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

case class TableSeq(tables: Seq[Table]) extends DomainObject {
  def getJson: JsonObject = Json.obj("tables" -> (tables map { t => Json.obj("id" -> t.id, "name" -> t.name) }))

  def setJson: JsonObject = Json.obj("tables" -> (tables map { t => Json.obj("id" -> t.id, "name" -> t.name) }))
}

case class CompleteTable(table: Table, columnList: Seq[ColumnType[_]], rowList: RowSeq) extends DomainObject {
  def getJson: JsonObject = table.getJson.mergeIn(Json.obj("columns" -> (columnList map { _.getJson.getArray("columns").get[JsonObject](0) }))).mergeIn(rowList.getJson)

  def setJson: JsonObject = table.setJson.mergeIn(Json.obj("columns" -> (columnList map { _.setJson.getArray("columns").get[JsonObject](0) }))).mergeIn(rowList.setJson)
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