package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.{LanguageType, TableauxDbType}
import org.joda.time.DateTime
import org.vertx.scala.core.json._

case class CellHistory(
    revision: Long,
//    rowId: RowId,
//    columnId: ColumnId,
    event: String,
    columnType: String,
    languageType: LanguageType,
    author: String,
    timestamp: Option[DateTime],
    value: JsonObject
) extends DomainObject {

  override def getJson: JsonObject = {
    val json = Json.obj(
      "revision" -> revision,
      "event" -> event,
      "columnType" -> columnType,
      "languageType" -> languageType.toString,
      "author" -> author,
      "value" -> value,
      "timestamp" -> optionToString(timestamp)
    )
    json.mergeIn(value)
  }
}

case class SeqCellHistory(rows: Seq[CellHistory]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("rows" -> (rows map (_.getJson)))
  }
}
