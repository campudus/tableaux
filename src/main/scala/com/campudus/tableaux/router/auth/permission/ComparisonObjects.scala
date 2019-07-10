package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.domain.{ColumnType, Table}

object ComparisonObjects {

  def apply(): ComparisonObjects = {
    new ComparisonObjects()
  }

  def apply(table: Table): ComparisonObjects = {
    new ComparisonObjects(Some(table))
  }

  def apply(table: Table, column: ColumnType[_]): ComparisonObjects = {
    new ComparisonObjects(Some(table), Some(column))
  }

  def apply(column: ColumnType[_]): ComparisonObjects = {
    new ComparisonObjects(columnOpt = Some(column))
  }

  def apply(column: ColumnType[_], value: Any): ComparisonObjects = {
    new ComparisonObjects(columnOpt = Some(column), valueOpt = Option(value))
  }

  def apply(table: Table, column: ColumnType[_], value: Any): ComparisonObjects = {
    new ComparisonObjects(Some(table), Some(column), Option(value))
  }
}

/**
  * Container for optional comparison objects. For example ScopeMedia doesn't need
  * any comparison object, while ScopeCell can have a Table, a Column and a Langtag.
  */
case class ComparisonObjects(
    // TODO after feature auth check for extreme simplification.
    //  If we only need table and column, there's no need for this class, because every column has a ref to its table.
    tableOpt: Option[Table] = None,
    columnOpt: Option[ColumnType[_]] = None,
    valueOpt: Option[Any] = None
//    langtag
//    cell
//    row
) {

  def merge(columnType: ColumnType[_]): ComparisonObjects = {
    new ComparisonObjects(this.tableOpt, Some(columnType))
  }
}
