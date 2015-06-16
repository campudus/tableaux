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

object Mapper {
  private def columnType(dbType: TableauxDbType): (Option[(Table, ColumnId, String, Ordering) => SimpleValueColumn[_]], TableauxDbType) = {
    dbType match {
      case TextType => (Some(StringColumn.apply), TextType)
      case NumericType => (Some(NumberColumn.apply), NumericType)

      // we can't handle this
      case AttachmentType => (None, AttachmentType)
      case LinkType => (None, LinkType)
    }
  }

  def apply(dbType: TableauxDbType): (Table, ColumnId, String, Ordering) => SimpleValueColumn[_] = columnType(dbType)._1.get

  def getDatabaseType(dbType: String): TableauxDbType = {
    dbType match {
      case TextType.name => TextType
      case NumericType.name => NumericType
      case LinkType.name => LinkType
      case AttachmentType.name => AttachmentType
    }
  }
}