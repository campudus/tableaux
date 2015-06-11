package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

sealed trait CreateColumn {
  val kind: TableauxDbType
}

case class CreateSimpleColumn(name: String, kind: TableauxDbType, ordering: Option[Ordering]) extends CreateColumn

case class CreateLinkColumn(name: String, ordering: Option[Ordering], linkConnection: LinkConnection) extends CreateColumn {
  override val kind = LinkType
}

case class CreateAttachmentColumn(name: String, ordering: Option[Ordering]) extends CreateColumn {
  override val kind: TableauxDbType = AttachmentType
}

sealed trait ColumnType[A] extends DomainObject {
  val kind: TableauxDbType

  val id: ColumnId

  val name: String

  val table: Table

  val ordering: Ordering

  override def getJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "name" -> name, "kind" -> kind.toString, "ordering" -> ordering)))

  override def setJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "ordering" -> ordering)))
}

sealed trait SimpleValueColumn[A] extends ColumnType[A]

case class StringColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[String] {
  override val kind = TextType
}

case class NumberColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[Number] {
  override val kind = NumericType
}

case class LinkColumn[A](table: Table, id: ColumnId, to: SimpleValueColumn[A], name: String, ordering: Ordering) extends ColumnType[Link[A]] {
  override val kind = LinkType

  override def getJson: JsonObject = Json.obj("columns" -> Json.arr(Json.obj("id" -> id, "name" -> name, "kind" -> kind.toString, "toTable" -> to.table.id, "toColumn" -> to.id, "ordering" -> ordering)))
}

case class AttachmentColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[String] {
  override val kind = AttachmentType
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