package com.campudus.tableaux.database.structure

import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._

sealed trait TableauxDbType

case object TextType extends TableauxDbType {
  override def toString: String = "text"
}

case object NumericType extends TableauxDbType {
  override def toString: String = "numeric"
}

case object LinkType extends TableauxDbType {
  override def toString: String = "link"
}

object Mapper {
  def columnType(s: TableauxDbType): (Option[(Table, IdType, String, Ordering) => ColumnValue[_]], TableauxDbType) = s match {
    case TextType => (Some(StringColumn.apply), TextType)
    case NumericType => (Some(NumberColumn.apply), NumericType)
    case LinkType => (None, LinkType)
  }

  def getApply(s: TableauxDbType): (Table, IdType, String, Ordering) => ColumnValue[_] = columnType(s)._1.get

  def getDatabaseType(s: String): TableauxDbType = s match {
    case "text" => TextType
    case "numeric" => NumericType
    case "link" => LinkType
  }
}