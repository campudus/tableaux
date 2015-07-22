package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.{AttachmentFile, TableauxModel}
import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

sealed trait ColumnType[+A] extends DomainObject {
  val kind: TableauxDbType

  val id: ColumnId

  val name: String

  val table: Table

  val ordering: Ordering

  override def getJson: JsonObject = Json.obj("id" -> id, "name" -> name, "kind" -> kind.toString, "ordering" -> ordering)

  override def setJson: JsonObject = Json.obj("id" -> id, "ordering" -> ordering)
}

sealed trait SimpleValueColumn[A] extends ColumnType[A]

case class TextColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[String] {
  override val kind = TextType
}

case class NumberColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[Number] {
  override val kind = NumericType
}

sealed trait MultiLanguageColumn[A] extends ColumnType[A] {
  override def getJson: JsonObject = super.getJson mergeIn Json.obj("multilanguage" -> true)
}

case class MultiTextColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends MultiLanguageColumn[String] {
  override val kind = TextType
}

case class MultiNumericColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends MultiLanguageColumn[Number] {
  override val kind = TextType
}

case class LinkColumn[A](table: Table, id: ColumnId, to: SimpleValueColumn[A], name: String, ordering: Ordering) extends ColumnType[Link[A]] {
  override val kind = LinkType

  override def getJson: JsonObject = super.getJson mergeIn Json.obj("toTable" -> to.table.id, "toColumn" -> to.id)
}

case class AttachmentColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends ColumnType[AttachmentFile] {
  override val kind = AttachmentType
}

case class ColumnSeq(columns: Seq[ColumnType[_]]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("columns" -> columns.map { _.getJson })

  override def setJson: JsonObject = Json.obj("columns" -> columns.map { _.setJson })
}