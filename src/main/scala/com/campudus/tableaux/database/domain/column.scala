package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.CheckValidValue._
import com.campudus.tableaux.database.model.AttachmentFile
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{LanguageNeutral, _}
import org.joda.time.{DateTime, LocalDate}
import org.vertx.scala.core.json._

// TODO We should put all infos into a ColumnInfo case class or similar: Every change in a column needs a lot of changes elsewhere
sealed trait ColumnType[+A] extends DomainObject {

  val kind: TableauxDbType

  val id: ColumnId

  val name: String

  val table: Table

  val ordering: Ordering

  val languageType: LanguageType

  val identifier: Boolean

  val displayInfos: Seq[DisplayInfo]

  override def getJson: JsonObject = {
    // backward compatibility
    val multilanguage: Boolean = languageType match {
      case _: LanguageNeutral => false
      case _ => true
    }

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

    languageType match {
      case _: MultiLanguage =>
        json.mergeIn(Json.obj("languageType" -> LanguageType.LANGUAGE))

      case MultiCountry(countryCodes) =>
        json.mergeIn(
          Json.obj(
            "languageType" -> LanguageType.COUNTRY,
            "countryCodes" -> Json.arr(countryCodes.codes: _*)
          )
        )

      case _: LanguageNeutral =>
      // do nothing
    }

    displayInfos.foreach { displayInfo =>
      displayInfo
        .optionalName
        .map(name => json.mergeIn(Json.obj("displayName" -> json.getJsonObject("displayName").mergeIn(Json.obj(displayInfo.langtag -> name)))))

      displayInfo
        .optionalDescription
        .map(desc => json.mergeIn(Json.obj("description" -> json.getJsonObject("description").mergeIn(Json.obj(displayInfo.langtag -> desc)))))
    }

    json
  }

  def checkValidValue[B](value: B): Option[String] = if (value == null) None else kind.checkValidValue(value)
}

/**
  * Helper for pattern matching
  */
object MultiLanguageColumn {
  def unapply(columnType: ColumnType[_]): Option[SimpleValueColumn[_]] = {
    (columnType, columnType.languageType) match {
      case (simpleValueColumn: SimpleValueColumn[_], _: MultiLanguage | _: MultiCountry) => Some(simpleValueColumn)
      case _ => None
    }
  }
}

/**
  * Base class for all primitive column types
  */
sealed abstract class SimpleValueColumn[+A](override val kind: TableauxDbType)(override val languageType: LanguageType) extends ColumnType[A] {

  override def checkValidValue[B](value: B): Option[String] = {
    languageType match {
      case _: LanguageNeutral => checkValidSingleValue(value)
      case _: MultiCountry | _: MultiLanguage => checkValidMultiValue(value)
    }
  }

  private def checkValidSingleValue[B](value: B): Option[String] = {
    if (value == null) None else kind.checkValidValue(value)
  }

  private def checkValidMultiValue[B](value: B): Option[String] = {
    try {
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
}

case class TextColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[String](TextType)(languageType)

case class ShortTextColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[String](ShortTextType)(languageType)

case class RichTextColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[String](RichTextType)(languageType)

case class NumberColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[Number](NumericType)(languageType)

case class CurrencyColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[Number](CurrencyType)(languageType)

case class BooleanColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[Boolean](BooleanType)(languageType)

case class DateColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[LocalDate](DateType)(languageType)

case class DateTimeColumn(override val languageType: LanguageType)(override val table: Table, override val id: ColumnId, override val name: String, override val ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends SimpleValueColumn[DateTime](DateTimeType)(languageType)

/*
 * Special column types
 */
case class LinkColumn[A](table: Table, id: ColumnId, to: ColumnType[A], linkId: LinkId, linkDirection: LinkDirection, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends ColumnType[Link[A]] {
  override val kind = LinkType
  override val languageType = to.languageType

  override def getJson: JsonObject = super.getJson mergeIn Json.obj("toTable" -> to.table.id, "toColumn" -> to.getJson)
}

case class AttachmentColumn(table: Table, id: ColumnId, name: String, ordering: Ordering, override val identifier: Boolean, override val displayInfos: Seq[DisplayInfo]) extends ColumnType[AttachmentFile] {
  override val kind = AttachmentType
  override val languageType = LanguageNeutral()
}

case class ConcatColumn(table: Table, name: String, columns: Seq[ColumnType[_]]) extends ColumnType[String] {
  override val kind = ConcatType

  // ConcatColumn will be a multi-language
  // column in case any columns is also multi-language
  override val languageType = !columns.exists(_.languageType match { case _: MultiLanguage | _: MultiCountry => true case _ => false }) match {
    case true => MultiLanguage()
    case false => LanguageNeutral()
  }

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