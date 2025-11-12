package com.campudus.tableaux.database.domain

import com.campudus.tableaux.Starter
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.UnionTableHelper

import org.vertx.scala.core.json._

object UnionTableRow {
  var rowOffset: Long = Starter.DEFAULT_UNION_TABLE_ROW_OFFSET
}

case class RawRow(
    id: RowId,
    rowLevelAnnotations: RowLevelAnnotations,
    rowPermissions: RowPermissions,
    cellLevelAnnotations: CellLevelAnnotations,
    values: Seq[_]
)

trait RowLike extends DomainObject {
  val table: Table
  val id: RowId
  val rowLevelAnnotations: RowLevelAnnotations
  val rowPermissions: RowPermissions
  val cellLevelAnnotations: CellLevelAnnotations
  val values: Seq[_]

  protected def buildBaseJson: JsonObject = {
    val json = Json.obj(
      "id" -> id,
      "values" -> compatibilityGet(values)
    )

    if (rowLevelAnnotations.isDefined) {
      json.mergeIn(rowLevelAnnotations.getJson)
    }

    if (cellLevelAnnotations.isDefined) {
      json.mergeIn(cellLevelAnnotations.getJson)
    }

    json
  }

  override def getJson: JsonObject = buildBaseJson
}

object RowLike {

  implicit class RowLikeOps(row: RowLike) {

    def toUnionTableRow(table: Table): UnionTableRow = {
      UnionTableRow(table, row.id, row.rowLevelAnnotations, row.rowPermissions, row.cellLevelAnnotations, row.values)
    }
  }
}

case class Row(
    table: Table,
    id: RowId,
    rowLevelAnnotations: RowLevelAnnotations,
    rowPermissions: RowPermissions,
    cellLevelAnnotations: CellLevelAnnotations,
    values: Seq[_]
) extends RowLike

case class UnionTableRow(
    table: Table,
    id: RowId,
    rowLevelAnnotations: RowLevelAnnotations,
    rowPermissions: RowPermissions,
    cellLevelAnnotations: CellLevelAnnotations,
    values: Seq[_]
) extends RowLike {

  override def getJson: JsonObject = {
    val json = buildBaseJson
    json.mergeIn(Json.obj(
      "tableId" -> table.id
    ))
    json.put("id", UnionTableHelper.calcRowId(table.id, id, UnionTableRow.rowOffset))
  }
}

case class RowSeq(rows: Seq[RowLike], page: Page = Page(Pagination(None, None), None)) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj(
      "page" -> compatibilityGet(page),
      "rows" -> (rows map (_.getJson))
    )
  }
}

case class DependentRows(table: Table, column: ColumnType[_], rows: Seq[JsonObject]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj(
      "table" -> table.getJson,
      "column" -> compatibilityGet(column),
      "rows" -> compatibilityGet(rows)
    )
  }
}

case class DependentRowsSeq(dependentRowsSeq: Seq[DependentRows]) extends DomainObject {

  override def getJson: JsonObject = Json.obj("dependentRows" -> compatibilityGet(dependentRowsSeq))
}
