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

  val multilanguage: Boolean = false

  override def getJson: JsonObject = Json.obj(
    "id" -> id,
    "ordering" -> ordering,
    "name" -> name,
    "kind" -> kind.toString,
    "multilanguage" -> multilanguage
  )

  override def setJson: JsonObject = Json.obj("id" -> id, "ordering" -> ordering)
}

sealed trait SimpleValueColumn[A] extends ColumnType[A]

object TextColumn {
  def apply(kind: TableauxDbType): (Table, ColumnId, String, Ordering) => TextColumn = {
    TextColumn(kind, _, _, _, _)
  }
}

case class TextColumn(override val kind: TableauxDbType, table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[String]

case class NumberColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[Number] {
  override val kind = NumericType
}

case class BooleanColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends SimpleValueColumn[Boolean] {
  override val kind = BooleanType
}

sealed trait MultiLanguageColumn[A] extends SimpleValueColumn[A] {
  override val multilanguage = true
}

case class MultiTextColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends MultiLanguageColumn[String] {
  override val kind = TextType
}

case class MultiNumericColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends MultiLanguageColumn[Number] {
  override val kind = NumericType
}

case class MultiBooleanColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends MultiLanguageColumn[Boolean] {
  override val kind = BooleanType
}

case class LinkColumn[A](table: Table, id: ColumnId, to: SimpleValueColumn[A], name: String, ordering: Ordering) extends ColumnType[Link[A]] {
  override val kind = LinkType

  override def getJson: JsonObject = super.getJson mergeIn Json.obj("toTable" -> to.table.id, "toColumn" -> to.getJson)
}

case class AttachmentColumn(table: Table, id: ColumnId, name: String, ordering: Ordering) extends ColumnType[AttachmentFile] {
  override val kind = AttachmentType
}

case class ColumnSeq(columns: Seq[ColumnType[_]]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("columns" -> columns.map { _.getJson })

  override def setJson: JsonObject = Json.obj("columns" -> columns.map { _.setJson })
}