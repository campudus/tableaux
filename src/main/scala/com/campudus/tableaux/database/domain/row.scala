package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

object UnionTableRow {
  val ROW_OFFSET = 1000000
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

  /**
    * Because the frontend and other API consumers cannot handle composite row IDs (tableId and rowId), we calculate a
    * unique row ID by adding a table-specific offset to the row ID. This is the easiest way to handle that problem
    * without changing the frontend and the API. We will encounter problems if we have more than 1,000,000 rows in a
    * single table, but that is highly unlikely today.
    */
  private def calcRowId(rowId: RowId, tableId: TableId): RowId =
    (tableId * UnionTableRow.ROW_OFFSET) + rowId

  override def getJson: JsonObject = {
    val json = buildBaseJson
    json.mergeIn(Json.obj(
      "tableId" -> table.id
    ))
    json.put("id", calcRowId(id, table.id))
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
