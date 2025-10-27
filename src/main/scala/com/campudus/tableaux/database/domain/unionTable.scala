package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.router.auth.permission.TableauxUser

import io.vertx.core.json.JsonObject
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

object UnionTableModel {

  def apply(
      tableId: TableId,
      columnId: ColumnId,
      originColumns: OriginColumns
  ): UnionTableModel = {
    new UnionTableModel(tableId, columnId, originColumns)
  }
}

case class UnionTableModel(
    tableId: TableId,
    columnId: TableId,
    originColumns: OriginColumns
)
