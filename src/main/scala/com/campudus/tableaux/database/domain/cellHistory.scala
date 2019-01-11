package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import org.joda.time.DateTime
import org.vertx.scala.core.json._

object BaseHistory {

  def apply(
      revision: Long,
      row_id: Long,
      column_id: Long,
      event: String,
      historyType: HistoryType,
      valueType: String,
      languageType: LanguageType,
      author: String,
      timestamp: Option[DateTime],
      value: JsonObject
  ): BaseHistory = {
    historyType match {
      case HistoryTypeCell | HistoryTypeComment =>
        CellHistory(revision, row_id, column_id, event, historyType, valueType, languageType, author, timestamp, value)
      case HistoryTypeCellFlag =>
        CellFlagHistory(revision, row_id, column_id, event, historyType, languageType, author, timestamp, value)
      case HistoryTypeRow =>
        RowHistory(revision, row_id, event, historyType, author, timestamp)
      case HistoryTypeRowFlag =>
        RowFlagHistory(revision, row_id, event, historyType, valueType, author, timestamp)
      case _ => throw new IllegalArgumentException("Invalid argument for CellHistory.apply")
    }
  }
}

sealed trait BaseHistory extends DomainObject {
  def revision: Long
  def row_id: Long
  def event: String
  def historyType: HistoryType
  def author: String
  def timestamp: Option[DateTime]

  override def getJson: JsonObject = {
    Json.obj(
      "revision" -> revision,
      "rowId" -> row_id,
      "event" -> event,
      "historyType" -> historyType.toString,
      "author" -> author,
      "timestamp" -> optionToString(timestamp)
    )
  }
}

case class RowHistory(
    override val revision: Long,
    override val row_id: Long,
    override val event: String,
    override val historyType: HistoryType,
    override val author: String,
    override val timestamp: Option[DateTime]
) extends BaseHistory {

  override def getJson: JsonObject = {
    super.getJson
  }
}

case class RowFlagHistory(
    override val revision: Long,
    override val row_id: Long,
    override val event: String,
    override val historyType: HistoryType,
    valueType: String,
    override val author: String,
    override val timestamp: Option[DateTime]
) extends BaseHistory {

  override def getJson: JsonObject = {
    super.getJson
      .mergeIn(
        Json.obj("valueType" -> valueType)
      )
  }
}

case class CellFlagHistory(
    override val revision: Long,
    override val row_id: Long,
    column_id: Long,
    override val event: String,
    override val historyType: HistoryType,
    languageType: LanguageType,
    override val author: String,
    override val timestamp: Option[DateTime],
    value: JsonObject
) extends BaseHistory {

  override def getJson: JsonObject = {
    super.getJson
      .mergeIn(
        Json.obj(
          "columnId" -> column_id,
          "languageType" -> languageType.toString,
          "value" -> Json.emptyObj()
        )
      )
      .mergeIn(value)
  }
}

case class CellHistory(
    override val revision: Long,
    override val row_id: Long,
    column_id: Long,
    override val event: String,
    override val historyType: HistoryType,
    valueType: String,
    languageType: LanguageType,
    override val author: String,
    override val timestamp: Option[DateTime],
    value: JsonObject
) extends BaseHistory {

  override def getJson: JsonObject = {
    super.getJson
      .mergeIn(
        Json.obj(
          "columnId" -> column_id,
          "valueType" -> valueType,
          "languageType" -> languageType.toString,
          "value" -> Json.emptyObj()
        )
      )
      .mergeIn(value)
  }
}

case class SeqCellHistory(rows: Seq[BaseHistory]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("rows" -> (rows map (_.getJson)))
  }
}
