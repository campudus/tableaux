package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

case class RawRow(
    id: RowId,
    rowLevelAnnotations: RowLevelAnnotations,
    rowPermissions: RowPermissions,
    cellLevelAnnotations: CellLevelAnnotations,
    values: Seq[_]
)

case class Row(
    table: Table,
    id: RowId,
    rowLevelAnnotations: RowLevelAnnotations,
    rowPermissions: RowPermissions,
    cellLevelAnnotations: CellLevelAnnotations,
    values: Seq[_]
) extends DomainObject {

  override def getJson: JsonObject = {
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
}

case class RowSeq(rows: Seq[Row], page: Page = Page(Pagination(None, None), None)) extends DomainObject {

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
