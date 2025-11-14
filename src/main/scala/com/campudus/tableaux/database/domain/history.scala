package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.History.RevisionId
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId}

import org.vertx.scala.core.json._

import org.joda.time.DateTime

sealed trait History extends DomainObject {
  val columnIdOpt: Option[ColumnId]
  val revision: RevisionId
  val rowId: RowId
  val event: String
  val historyType: HistoryType
  val author: String
  val timestamp: Option[DateTime]
  val deletedAt: Option[DateTime]
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

  implicit class HistoryOps(h: History) {

    // def toUnionTableHistory(originTableRowId: RowId): History = {
    //   val baseHistory =
    //     BaseHistory(h.revision, originTableRowId, h.event, h.historyType, h.author, h.timestamp, h.deletedAt)

    //   h match {
    //     // case ch: CellHistory =>
    //     //   val columnId = originColumnIdOpt.getOrElse(ch.columnId)
    //     //   CellHistory(baseHistory, columnId, ch.valueType, ch.languageType, ch.value)
    //     // case cfh: CellFlagHistory =>
    //     //   val columnId = originColumnIdOpt.getOrElse(cfh.columnId)
    //     // CellFlagHistory(baseHistory, columnId, cfh.languageType, cfh.value)
    //     case rh: RowHistory => RowHistory(baseHistory, rh.value)
    //     case rf: RowFlagHistory => RowFlagHistory(baseHistory, rf.valueType)
    //     case rph: RowPermissionsHistory => RowPermissionsHistory(baseHistory, rph.valueType, rph.value)
    //     case _ => throw new IllegalArgumentException(
    //         s"Invalid historyType for toUnionTableHistory: ${h.getClass.getSimpleName}"
    //       )
    //   }
    // }

    /**
      * In order to map the history entries to the cells of the union table in FE, we must overwrite the rowId and
      * columnId of the history with those of the UnionTable.
      */
    def toUnionTableHistory(unionRowId: RowId, unionColumnId: ColumnId): History = {
      val baseHistory =
        BaseHistory(h.revision, unionRowId, h.event, h.historyType, h.author, h.timestamp, h.deletedAt)

      h match {
        case ch: CellHistory => CellHistory(baseHistory, unionColumnId, ch.valueType, ch.languageType, ch.value)
        case cfh: CellFlagHistory => CellFlagHistory(baseHistory, unionColumnId, cfh.languageType, cfh.value)
        case rh: RowHistory => RowHistory(baseHistory, rh.value)
        case rf: RowFlagHistory => RowFlagHistory(baseHistory, rf.valueType)
        case rph: RowPermissionsHistory => RowPermissionsHistory(baseHistory, rph.valueType, rph.value)
        case _ => throw new IllegalArgumentException(
            s"Invalid historyType for toUnionTableHistory: ${h.getClass.getSimpleName}"
          )
      }
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

trait HistoryWithBaseHistory extends History {
  val baseHistory: BaseHistory

  override val revision: RevisionId = baseHistory.revision
  override val rowId: RowId = baseHistory.rowId
  override val event: String = baseHistory.event
  override val historyType: HistoryType = baseHistory.historyType
  override val author: String = baseHistory.author
  override val timestamp: Option[DateTime] = baseHistory.timestamp
  override val deletedAt: Option[DateTime] = baseHistory.deletedAt
}

case class RowFlagHistory(baseHistory: BaseHistory, valueType: String) extends HistoryWithBaseHistory {

  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(
        Json.obj("valueType" -> valueType)
      )
  }

  override val columnIdOpt: Option[ColumnId] = None
}

case class RowPermissionsHistory(baseHistory: BaseHistory, valueType: String, value: JsonObject)
    extends HistoryWithBaseHistory {

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

case class RowHistory(baseHistory: BaseHistory, value: JsonObject) extends HistoryWithBaseHistory {

  override def getJson: JsonObject = {
    baseHistory.getJson
      .mergeIn(value)
  }

  override val columnIdOpt: Option[ColumnId] = None
}

case class CellFlagHistory(baseHistory: BaseHistory, columnId: ColumnId, languageType: LanguageType, value: JsonObject)
    extends HistoryWithBaseHistory {

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
) extends HistoryWithBaseHistory {

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
