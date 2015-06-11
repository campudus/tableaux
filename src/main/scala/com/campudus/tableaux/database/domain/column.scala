package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.{AttachmentType, LinkType, TableauxDbType}
import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

sealed trait CreateColumn {
  val kind: TableauxDbType
}

case class CreateSimpleColumn(name: String, kind: TableauxDbType, ordering: Option[Ordering]) extends CreateColumn

case class CreateLinkColumn(name: String, ordering: Option[Ordering], linkConnections: Option[LinkConnection]) extends CreateColumn {
  override val kind = LinkType
}

case class CreateAttachmentColumn(name: String, ordering: Option[Ordering]) extends CreateColumn {
  override val kind: TableauxDbType = AttachmentType
}

sealed trait ColumnType[A] extends DomainObject {
  val dbType: String

  val id: ColumnId

  val name: String

  val table: Table

  val ordering: Ordering

  override def getJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "name" -> name, "kind" -> dbType, "ordering" -> ordering)))

  override def setJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "ordering" -> ordering)))
}

sealed trait LinkColumnType[A] extends ColumnType[Link[A]] {
  def to: ColumnValue[A]

  override def getJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "name" -> name, "kind" -> dbType, "toTable" -> to.table.id, "toColumn" -> to.id, "ordering" -> ordering)))
}

sealed trait ColumnValue[A] extends ColumnType[A]

case class StringColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends ColumnValue[String] {
  override val dbType = "text"
}

case class NumberColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends ColumnValue[Number] {
  override val dbType = "numeric"
}

case class LinkColumn[A](table: Table, id: ColumnId, to: ColumnValue[A], name: String, ordering: Ordering) extends LinkColumnType[A] {
  override val dbType = "link"
}

case class AttachmentColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends ColumnValue[String] {
  override val dbType = "attachment"
}

case class ColumnSeq(columns: Seq[ColumnType[_]]) extends DomainObject {
  def getJson: JsonObject = Json.obj("columns" ->
    (columns map {
      col =>
        col.getJson.getArray("columns").get[JsonObject](0)
    }))

  def setJson: JsonObject = Json.obj("columns" ->
    (columns map {
      col =>
        col.setJson.getArray("columns").get[JsonObject](0)
    }))
}