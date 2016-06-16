package com.campudus.tableaux.database

import java.util.Date

import com.campudus.tableaux.database.CheckValidValue._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import io.vertx.core.json.JsonArray
import org.joda.time.DateTime
import org.vertx.scala.core.json.JsonObject

import scala.util.{Success, Try}

sealed trait TableauxDbType {
  val name: String

  override def toString: String = name

  def toDbType: String = name

  def checkValidValue[B](value: B): Option[String]
}

case object TextType extends TableauxDbType {
  override val name = "text"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[String])
}

case object RichTextType extends TableauxDbType {
  override val name = "richtext"

  override def toDbType = "text"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[String])
}

case object ShortTextType extends TableauxDbType {
  override val name = "shorttext"

  override def toDbType = "text"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[String])
}

case object NumericType extends TableauxDbType {
  override val name = "numeric"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[Number])
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

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[JsonArray] || value.isInstanceOf[JsonObject])
}

case object BooleanType extends TableauxDbType {
  override val name = "boolean"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[Boolean])
}

case object DateType extends TableauxDbType {
  override val name = "date"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[String] || value.isInstanceOf[Date])
}

case object DateTimeType extends TableauxDbType {
  override val name = "datetime"

  override def toDbType = "timestamp with time zone"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[String] || value.isInstanceOf[DateTime])
}

case object ConcatType extends TableauxDbType {
  override val name = "concat"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[JsonArray])
}

case object CurrencyType extends TableauxDbType {
  override val name = "currency"

  override def toDbType = "numeric"

  override def checkValidValue[B](value: B): Option[String] = boolToOption(value.isInstanceOf[Number])
}

object Mapper {
  private def columnType(kind: TableauxDbType, languageType: LanguageType): Option[(Table, ColumnId, String, Ordering, Boolean, Seq[DisplayInfo]) => ColumnType[_]] = {
    kind match {
      // primitive/simple types
      case TextType => Some(TextColumn(languageType))
      case RichTextType => Some(RichTextColumn(languageType))
      case ShortTextType => Some(ShortTextColumn(languageType))
      case NumericType => Some(NumberColumn(languageType))
      case CurrencyType => Some(CurrencyColumn(languageType))
      case BooleanType => Some(BooleanColumn(languageType))
      case DateType => Some(DateColumn(languageType))
      case DateTimeType => Some(DateTimeColumn(languageType))

      // complex types e.g AttachmentType
      case _ => None
    }
  }

  def apply(languageType: LanguageType, kind: TableauxDbType): (Table, ColumnId, String, Ordering, Boolean, Seq[DisplayInfo]) => ColumnType[_] = columnType(kind, languageType).get

  def getDatabaseType(kind: String): TableauxDbType = {
    kind match {
      case TextType.name => TextType
      case ShortTextType.name => ShortTextType
      case RichTextType.name => RichTextType
      case NumericType.name => NumericType
      case CurrencyType.name => CurrencyType
      case LinkType.name => LinkType
      case AttachmentType.name => AttachmentType
      case BooleanType.name => BooleanType
      case DateType.name => DateType
      case DateTimeType.name => DateTimeType
    }
  }
}