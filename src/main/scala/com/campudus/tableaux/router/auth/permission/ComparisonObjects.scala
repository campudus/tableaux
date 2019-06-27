package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.domain.{ColumnType, Table}

object ComparisonObjects {

  def apply(): ComparisonObjects = {
    new ComparisonObjects(None, None)
  }

  def apply(table: Table): ComparisonObjects = {
    new ComparisonObjects(tableOpt = Some(table), None)
  }

  def apply(table: Table, column: ColumnType[_]): ComparisonObjects = {
    new ComparisonObjects(tableOpt = Some(table), columnOpt = Some(column))
  }
}

/**
  * Container for optional comparison objects. For example ScopeMedia doesn't need
  * any comparison object, while ScopeCell can have a Table, a Column and a Langtag.
  */
case class ComparisonObjects(
    tableOpt: Option[Table],
    columnOpt: Option[ColumnType[_]]
//    langtag
//    cell
//    row
)
