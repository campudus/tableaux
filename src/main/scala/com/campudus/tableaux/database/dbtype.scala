package com.campudus.tableaux.database

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._

sealed trait TableauxDbType {
  val name: String

  override def toString: String = name

  def toDbType: String = name
}

case object TextType extends TableauxDbType {
  override val name = "text"
}

case object RichTextType extends TableauxDbType {
  override val name = "richtext"

  override def toDbType = "text"
}

case object ShortTextType extends TableauxDbType {
  override val name = "shorttext"

  override def toDbType = "text"
}

case object NumericType extends TableauxDbType {
  override val name = "numeric"
}

case object LinkType extends TableauxDbType {
  override val name = "link"
}

case object AttachmentType extends TableauxDbType {
  override val name = "attachment"
}

case object BooleanType extends TableauxDbType {
  override val name = "boolean"
}

case object DateType extends TableauxDbType {
  override val name = "date"
}

case object DateTimeType extends TableauxDbType {
  override val name = "datetime"

  override def toDbType = "timestamp with time zone"
}

case object ConcatType extends TableauxDbType {
  override val name = "concat"
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