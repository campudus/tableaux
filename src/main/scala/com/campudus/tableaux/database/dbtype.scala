package com.campudus.tableaux.database

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._

sealed trait TableauxDbType {
  val name: String

  override def toString: String = name
}

case object TextType extends TableauxDbType {
  override val name = "text"
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
  private def columnType(languageType: LanguageType, kind: TableauxDbType): Option[(Table, ColumnId, String, Ordering) => ColumnType[_]] = {
    languageType match {
      case SingleLanguage => kind match {
        // primitive/simple types
        case TextType => Some(TextColumn.apply)
        case NumericType => Some(NumberColumn.apply)

        // complex types
        case AttachmentType => None
        case LinkType => None
      }

      case MultiLanguage => kind match {
        // primitive/simple types
        case TextType => Some(MultiTextColumn.apply)
        case NumericType => Some(MultiNumericColumn.apply)

        // complex types
        case AttachmentType => None
        case LinkType => None
      }
    }
  }

  def apply(languageType: LanguageType, kind: TableauxDbType): (Table, ColumnId, String, Ordering) => ColumnType[_] = columnType(languageType, kind).get

  def getDatabaseType(kind: String): TableauxDbType = {
    kind match {
      case TextType.name => TextType
      case NumericType.name => NumericType
      case LinkType.name => LinkType
      case AttachmentType.name => AttachmentType
    }
  }
}