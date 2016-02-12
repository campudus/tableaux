package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.CheckValidValue._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.AttachmentFile
import com.campudus.tableaux.database.model.TableauxModel._
import org.joda.time.{DateTime, LocalDate}
import org.vertx.scala.core.json._

// TODO We should put all infos into a ColumnInfo case class or similar: Every change in a column needs a lot of changes elsewhere
sealed trait ColumnType[+A] extends DomainObject {
  val kind: TableauxDbType

  val id: ColumnId

  val name: String

  val table: Table

  val ordering: Ordering

  val multilanguage: Boolean

  val identifier: Boolean = false

  val displayInfos: Seq[DisplayInfo]

  override def getJson: JsonObject = {
    val json = Json.obj(
      "id" -> id,
      "ordering" -> ordering,
      "name" -> name,
      "kind" -> kind.toString,
      "multilanguage" -> multilanguage,
      "identifier" -> identifier,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )
    displayInfos.foreach { di =>
      di.optionalName.map(name => json.mergeIn(Json.obj("displayName" -> json.getJsonObject("displayName").mergeIn(Json.obj(di.langtag -> name)))))
      di.optionalDescription.map(desc => json.mergeIn(Json.obj("description" -> json.getJsonObject("description").mergeIn(Json.obj(di.langtag -> desc)))))
    }
    json
  }

  def checkValidValue[B](value: B): Option[String] = if (value == null) None else kind.checkValidValue(value)
}

sealed trait ValueColumn[+A] extends ColumnType[A]

/*
 * Single-language column types
 */
sealed trait SimpleValueColumn[+A] extends ValueColumn[A] {
  override val multilanguage = false
}

object TextColumn {
  def apply(kind: TableauxDbType): (Table, ColumnId, String, Ordering, Boolean, Seq[DisplayInfo]) => TextColumn = {
    TextColumn(kind, _, _, _, _, _, _)
  }
}

case class TextColumn(override val kind: TableauxDbType, table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[String]

case class NumberColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[Number] {
  override val kind = NumericType
}

case class BooleanColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[Boolean] {
  override val kind = BooleanType
}

case class DateColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[LocalDate] {
  override val kind = DateType
}

case class DateTimeColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[DateTime] {
  override val kind = DateTimeType
}

/*
 * Multi-language column types
 */
sealed trait MultiLanguageColumn[A] extends ValueColumn[A] {
  override val multilanguage = true

  override def checkValidValue[B](value: B): Option[String] = try {
    import scala.collection.JavaConverters._
    val json = value.asInstanceOf[JsonObject]
    val map = json.getMap
    map.entrySet().asScala.toStream.map { entry =>
      if (entry.getValue == null) None else kind.checkValidValue(entry.getValue)
    }.find(_.isDefined).flatten
  } catch {
    case _: Throwable => boolToOption(false)
  }
}

object MultiTextColumn {
  def apply(kind: TableauxDbType): (Table, ColumnId, String, Ordering, Boolean, Seq[DisplayInfo]) => MultiTextColumn = {
    MultiTextColumn(kind, _, _, _, _, _, _)
  }
}

case class MultiTextColumn(override val kind: TableauxDbType, table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends MultiLanguageColumn[String]

case class MultiNumericColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends MultiLanguageColumn[Number] {
  override val kind = NumericType
}

case class MultiBooleanColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends MultiLanguageColumn[Boolean] {
  override val kind = BooleanType
}

case class MultiDateColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends MultiLanguageColumn[LocalDate] {
  override val kind = DateType
}

case class MultiDateTimeColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends MultiLanguageColumn[DateTime] {
  override val kind = DateTimeType
}

/*
 * Special column types
 */
case class LinkColumn[A](table: Table, id: ColumnId, to: ValueColumn[A], linkId: LinkId, linkDirection: LinkDirection, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends ColumnType[Link[A]] {
  override val kind = LinkType
  override val multilanguage = to.multilanguage

  override def getJson: JsonObject = super.getJson mergeIn Json.obj("toTable" -> to.table.id, "toColumn" -> to.getJson)
}

case class AttachmentColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends ColumnType[AttachmentFile] {
  override val kind = AttachmentType
  override val multilanguage = false
}

case class ConcatColumn(table: Table, name: String, columns: Seq[ColumnType[_]]) extends ValueColumn[String] {
  override val kind = ConcatType

  // ConcatColumn will be a multi-language
  // column in case any columns is also multi-language
  override val multilanguage = columns.exists(_.multilanguage)

  // Right now, every concat column is
  // an identifier
  override val identifier = true

  override val id: ColumnId = 0
  override val ordering: Ordering = 0
  override val displayInfos: Seq[DisplayInfo] = List()

  override def getJson: JsonObject = super.getJson mergeIn Json.obj("concats" -> columns.map(_.getJson))
}

/**
  * Column seq is just a sequence of columns.
  *
  * @param columns The sequence of columns.
  */
case class ColumnSeq(columns: Seq[ColumnType[_]]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("columns" -> columns.map(_.getJson))
}