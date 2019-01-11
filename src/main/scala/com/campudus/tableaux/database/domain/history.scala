package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.History.RevisionId
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId}
import org.joda.time.DateTime
import org.vertx.scala.core.json._

sealed trait History extends DomainObject

object History {
  type RevisionId = Long

  def apply(
      revision: RevisionId,
      rowId: RowId,
      columnId: ColumnId,
      event: String,
      historyType: HistoryType,
      valueType: String,
      languageType: LanguageType,
      author: String,
      timestamp: Option[DateTime],
      value: JsonObject
  ): History = {
    val baseHistory = BaseHistory(revision, rowId, event, historyType, author, timestamp)
    historyType match {
      case HistoryTypeCell | HistoryTypeComment => CellHistory(baseHistory, columnId, valueType, languageType, value)
      case HistoryTypeCellFlag => CellFlagHistory(baseHistory, columnId, languageType, value)
      case HistoryTypeRow => baseHistory
      case HistoryTypeRowFlag => RowFlagHistory(baseHistory, valueType)
      case _ => throw new IllegalArgumentException(s"Invalid historyType for CellHistory.apply $historyType")
    }
  }
}

case class BaseHistory(
    revision: RevisionId,
    rowId: RowId,
    event: String,
    historyType: HistoryType,
    author: String,
    timestamp: Option[DateTime]
) extends History {

  override def getJson: JsonObject = {
    Json.obj(
      "revision" -> revision,
      "rowId" -> rowId,
      "event" -> event,
      "historyType" -> historyType.toString,
      "author" -> author,
      "timestamp" -> optionToString(timestamp)
    )
  }
}

case class RowFlagHistory(baseHistory: BaseHistory, valueType: String) extends History {
  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(
        Json.obj("valueType" -> valueType)
      )
  }
}

case class CellFlagHistory(baseHistory: BaseHistory, columnId: ColumnId, languageType: LanguageType, value: JsonObject)
    extends History {
  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(
        Json.obj(
          "columnId" -> columnId,
          "languageType" -> languageType.toString,
          "value" -> Json.emptyObj()
        )
      )
      .mergeIn(value)
  }
}

case class CellHistory(
    baseHistory: BaseHistory,
    columnId: ColumnId,
    valueType: String,
    languageType: LanguageType,
    value: JsonObject
) extends History {

  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(
        Json.obj(
          "columnId" -> columnId,
          "valueType" -> valueType,
          "languageType" -> languageType.toString,
          "value" -> Json.emptyObj()
        )
      )
      .mergeIn(value)
  }
}

case class SeqHistory(rows: Seq[History]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("rows" -> (rows map (_.getJson)))
  }
}
