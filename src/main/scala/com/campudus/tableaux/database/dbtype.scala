package com.campudus.tableaux.database

import java.util.Date

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import io.vertx.core.json.{JsonObject, JsonArray}
import org.joda.time.DateTime

import scala.util.{Success, Try}

import CheckValidValue._

sealed trait TableauxDbType {
  val name: String

  override def toString: String = name

  def toDbType: String = name

  def checkValidValue[B](value: B): Option[String]
}

case object TextType extends TableauxDbType {
  override val name = "text"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[String])
}

case object RichTextType extends TableauxDbType {
  override val name = "richtext"

  override def toDbType = "text"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[String])
}

case object ShortTextType extends TableauxDbType {
  override val name = "shorttext"

  override def toDbType = "text"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[String])
}

case object NumericType extends TableauxDbType {
  override val name = "numeric"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[Number])
}

case object LinkType extends TableauxDbType {
  override val name = "link"

  private val fail: Option[String] = Some("link-value")

  override def checkValidValue[B](value: B): Option[String] = value match {
    case obj: JsonObject => Try(obj.getLong("to")) match {
      case Success(l) => None
      case _ => fail
    }
    case arr: JsonArray =>
      import scala.collection.JavaConverters._
      if (arr.getList.asScala.forall(_.isInstanceOf[Long])) {
        None
      } else {
        fail
      }
    case _ => fail
  }
}

case object AttachmentType extends TableauxDbType {
  override val name = "attachment"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[JsonArray] || value.isInstanceOf[JsonObject])
}

case object BooleanType extends TableauxDbType {
  override val name = "boolean"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[Boolean])
}

case object DateType extends TableauxDbType {
  override val name = "date"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[String] || value.isInstanceOf[Date])
}

case object DateTimeType extends TableauxDbType {
  override val name = "datetime"

  override def toDbType = "timestamp with time zone"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[String] || value.isInstanceOf[DateTime])
}

case object ConcatType extends TableauxDbType {
  override val name = "concat"

  override def checkValidValue[B](value: B): Option[String] = boolToArgumentError(value.isInstanceOf[JsonArray])
}

sealed trait LanguageType {
  def toBoolean: Boolean
}

object LanguageType {
  def apply(multiLanguage: Boolean): LanguageType = {
    if (multiLanguage) {
      MultiLanguage
    } else {
      SingleLanguage
    }
  }
}

case object SingleLanguage extends LanguageType {
  override def toBoolean: Boolean = false
}

case object MultiLanguage extends LanguageType {
  override def toBoolean: Boolean = true
}

object Mapper {
  private def columnType(languageType: LanguageType, kind: TableauxDbType): Option[(Table, ColumnId, String, Ordering, Boolean) => ColumnType[_]] = {
    languageType match {
      case SingleLanguage => kind match {
        // primitive/simple types
        case TextType | RichTextType | ShortTextType => Some(TextColumn(kind))
        case NumericType => Some(NumberColumn.apply)
        case BooleanType => Some(BooleanColumn.apply)
        case DateType => Some(DateColumn.apply)
        case DateTimeType => Some(DateTimeColumn.apply)

        // complex types
        case AttachmentType => None
        case LinkType => None
        case ConcatType => None
      }

      case MultiLanguage => kind match {
        // primitive/simple types
        case TextType | RichTextType | ShortTextType => Some(MultiTextColumn(kind))
        case NumericType => Some(MultiNumericColumn.apply)
        case BooleanType => Some(MultiBooleanColumn.apply)
        case DateType => Some(MultiDateColumn.apply)
        case DateTimeType => Some(MultiDateTimeColumn.apply)

        // complex types
        case AttachmentType => None
        case LinkType => None
        case ConcatType => None
      }
    }
  }

  def apply(languageType: LanguageType, kind: TableauxDbType): (Table, ColumnId, String, Ordering, Boolean) => ColumnType[_] = columnType(languageType, kind).get

  def getDatabaseType(kind: String): TableauxDbType = {
    kind match {
      case TextType.name => TextType
      case ShortTextType.name => ShortTextType
      case RichTextType.name => RichTextType
      case NumericType.name => NumericType
      case LinkType.name => LinkType
      case AttachmentType.name => AttachmentType
      case BooleanType.name => BooleanType
      case DateType.name => DateType
      case DateTimeType.name => DateTimeType
    }
  }
}