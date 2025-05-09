package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.History.RevisionId
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId}

import org.vertx.scala.core.json._

import org.joda.time.DateTime

sealed trait History extends DomainObject {
  val columnIdOpt: Option[ColumnId]
}

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
      value: JsonObject,
      deletedAt: Option[DateTime]
  ): History = {
    val baseHistory = BaseHistory(revision, rowId, event, historyType, author, timestamp, deletedAt)
    historyType match {
      case HistoryTypeCell | HistoryTypeCellComment =>
        CellHistory(baseHistory, columnId, valueType, languageType, value)
      case HistoryTypeCellFlag => CellFlagHistory(baseHistory, columnId, languageType, value)
      case HistoryTypeRow => RowHistory(baseHistory, value)
      case HistoryTypeRowFlag => RowFlagHistory(baseHistory, valueType)
      case HistoryTypeRowPermissions => RowPermissionsHistory(baseHistory, valueType, value)
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
    timestamp: Option[DateTime],
    deletedAt: Option[DateTime]
) extends History {

  override def getJson: JsonObject = {
    val deletedAtJson = optionToString(deletedAt) match {
      case null => Json.emptyObj()
      case v => Json.obj("deletedAt" -> v)
    }

    Json.obj(
      "revision" -> revision,
      "rowId" -> rowId,
      "event" -> event,
      "historyType" -> historyType.toString,
      "author" -> author,
      "timestamp" -> optionToString(timestamp)
    ).mergeIn(deletedAtJson)
  }

  override val columnIdOpt: Option[ColumnId] = None
}

case class RowFlagHistory(baseHistory: BaseHistory, valueType: String) extends History {

  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(
        Json.obj("valueType" -> valueType)
      )
  }

  override val columnIdOpt: Option[ColumnId] = None
}

case class RowPermissionsHistory(baseHistory: BaseHistory, valueType: String, value: JsonObject) extends History {

  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(
        Json.obj(
          "valueType" -> valueType,
          "value" -> Json.emptyObj()
        )
      ).mergeIn(value)
  }

  override val columnIdOpt: Option[ColumnId] = None
}

case class RowHistory(baseHistory: BaseHistory, value: JsonObject) extends History {

  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(value)
  }

  override val columnIdOpt: Option[ColumnId] = None
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

  override val columnIdOpt: Option[ColumnId] = Some(columnId)
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

  override val columnIdOpt: Option[ColumnId] = Some(columnId)
}

case class SeqHistory(rows: Seq[History]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("rows" -> (rows map (_.getJson)))
  }
}
