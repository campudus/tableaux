package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.CheckValidValue._
import com.campudus.tableaux.database.model.AttachmentFile
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{LanguageNeutral, _}
import org.joda.time.{DateTime, LocalDate}
import org.vertx.scala.core.json._

sealed trait ColumnInformation {
  val table: Table
  val id: ColumnId
  val name: String
  val ordering: Ordering
  val identifier: Boolean
  val displayInfos: Seq[DisplayInfo]
}

case class BasicColumnInformation(override val table: Table,
                                  override val id: ColumnId,
                                  override val name: String,
                                  override val ordering: Ordering,
                                  override val identifier: Boolean,
                                  override val displayInfos: Seq[DisplayInfo]) extends ColumnInformation

case class ConcatColumnInformation(override val table: Table) extends ColumnInformation {
  override val name = "ID"

  // Right now, every concat column is
  // an identifier
  override val identifier = true

  override val id: ColumnId = 0
  override val ordering: Ordering = 0
  override val displayInfos: Seq[DisplayInfo] = List()
}

sealed trait ColumnType[+A] extends DomainObject {

  val kind: TableauxDbType

  val languageType: LanguageType

  protected val columnInformation: ColumnInformation

  final val table: Table = columnInformation.table

  final val id: ColumnId = columnInformation.id

  final val name: String = columnInformation.name

  final val ordering: Ordering = columnInformation.ordering

  final val identifier: Boolean = columnInformation.identifier

  override def getJson: JsonObject = {
    // backward compatibility
    val multilanguage = languageType != LanguageNeutral

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
      case MultiLanguage =>
        json.mergeIn(Json.obj("languageType" -> LanguageType.LANGUAGE))

      case MultiCountry(countryCodes) =>
        json.mergeIn(
          Json.obj(
            "languageType" -> LanguageType.COUNTRY,
            "countryCodes" -> Json.arr(countryCodes.codes: _*)
          )
        )

      case _ =>
      // do nothing
    }

    columnInformation.displayInfos.foreach { displayInfo =>
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
      case (simpleValueColumn: SimpleValueColumn[_], MultiLanguage | MultiCountry(_)) => Some(simpleValueColumn)
      case _ => None
    }
  }
}

object SimpleValueColumn {
  def apply(kind: TableauxDbType, languageType: LanguageType, columnInformation: ColumnInformation): SimpleValueColumn[_] = {
    val applyFn: (LanguageType) => (ColumnInformation) => SimpleValueColumn[_] = kind match {
      case TextType => TextColumn.apply
      case RichTextType => RichTextColumn.apply
      case ShortTextType => ShortTextColumn.apply
      case NumericType => NumberColumn.apply
      case CurrencyType => CurrencyColumn.apply
      case BooleanType => BooleanColumn.apply
      case DateType => DateColumn.apply
      case DateTimeType => DateTimeColumn.apply

      case _ => throw new IllegalArgumentException("Can only map type to SimpleValueColumn")
    }

    applyFn(languageType)(columnInformation)
  }
}

/**
  * Base class for all primitive column types
  */
sealed abstract class SimpleValueColumn[+A](override val kind: TableauxDbType)(override val languageType: LanguageType) extends ColumnType[A] {

  override def checkValidValue[B](value: B): Option[String] = {
    languageType match {
      case MultiLanguage | MultiCountry(_) => checkValidMultiValue(value)
      case _ => checkValidSingleValue(value)
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

case class TextColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[String](TextType)(languageType)

case class ShortTextColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[String](ShortTextType)(languageType)

case class RichTextColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[String](RichTextType)(languageType)

case class NumberColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[Number](NumericType)(languageType)

case class CurrencyColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[Number](CurrencyType)(languageType)

case class BooleanColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[Boolean](BooleanType)(languageType)

case class DateColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[LocalDate](DateType)(languageType)

case class DateTimeColumn(override val languageType: LanguageType)(override val columnInformation: ColumnInformation) extends SimpleValueColumn[DateTime](DateTimeType)(languageType)

/*
 * Special column types
 */
case class LinkColumn[A](override val columnInformation: ColumnInformation, to: ColumnType[A], linkId: LinkId, linkDirection: LinkDirection) extends ColumnType[Link[A]] {
  override val kind = LinkType
  override val languageType = to.languageType

  override def getJson: JsonObject = super.getJson mergeIn Json.obj("toTable" -> to.table.id, "toColumn" -> to.getJson)
}

case class AttachmentColumn(override val columnInformation: ColumnInformation) extends ColumnType[AttachmentFile] {
  override val kind = AttachmentType
  override val languageType = LanguageNeutral
}

case class ConcatColumn(override val columnInformation: ConcatColumnInformation, columns: Seq[ColumnType[_]]) extends ColumnType[String] {
  override val kind = ConcatType

  // If any of the columns is MultiLanguage or MultiCountry
  // the ConcatColumn will be MultiLanguage
  override val languageType = {
    val isMultiLanguageOrMultiCountry = columns.forall(_.languageType match {
      case MultiLanguage | MultiCountry(_) => false
      case _ => true
    })

    if (isMultiLanguageOrMultiCountry) {
      MultiLanguage
    } else {
      LanguageNeutral
    }
  }

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