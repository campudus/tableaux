package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.TableauxDbType
import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

case class CreateColumn(name: String, kind: TableauxDbType, ordering: Option[Ordering], linkConnections: Option[LinkConnection])

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
  def getJson: JsonObject = Json.obj("columns" ->
    (columns map {
      case c: LinkColumnType[_] => Json.obj("id" -> c.id, "name" -> c.name, "kind" -> c.dbType, "toTable" -> c.to.table.id, "toColumn" -> c.to.id, "ordering" -> c.ordering)
      case c: ColumnValue[_] => Json.obj("id" -> c.id, "name" -> c.name, "kind" -> c.dbType, "ordering" -> c.ordering)
    }))
  def setJson: JsonObject = Json.obj("columns" -> (columns map { col => Json.obj("id" -> col.id, "ordering" -> col.ordering) }))
}