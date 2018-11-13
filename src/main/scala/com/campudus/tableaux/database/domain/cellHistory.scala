package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class CellHistory(
    revision: Long,
//    rowId: RowId,
//    columnId: ColumnId,
//    event: String,
//  columnType:
//  multilanguage:
//  author:
//  timestamp:
    value: JsonObject
) extends DomainObject {

  override def getJson: JsonObject = {
    val json = Json.obj(
      "revision" -> revision,
//      "rowId" -> rowId,
//      "columnId" -> columnId,
//      "event" -> event
      "value" -> value
    )
    json
  }
}

case class SeqCellHistory(rows: Seq[CellHistory]) extends DomainObject {

  override def getJson: JsonObject = {
//    Json.obj(
////      "page" -> compatibilityGet(page),
//      "rows" -> (rows map (_.getJson))
//    )
    Json.obj("rows" -> (rows map (_.getJson)))
  }
}
