package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import org.joda.time.DateTime
import org.vertx.scala.core.json._

import scala.util.Try

object CellHistory {

  def apply(
      revision: Long,
      row_id: Long,
      column_id: Long,
      event: String,
      historyType: String,
      languageType: LanguageType,
      author: String,
      timestamp: Option[DateTime],
      value: JsonObject
  ): CellHistory = {
    HistoryEventType(event) match {
      case CellChangedEvent =>
        CellChangedHistory(revision, row_id, column_id, event, author, timestamp, historyType, languageType, value)
      case RowCreatedEvent => RowCreatedHistory(revision, row_id, event, author, timestamp)
      case _ => throw new IllegalArgumentException("Invalid argument for CellHistory.apply")
    }
  }
}

sealed trait CellHistory extends DomainObject {
  val revision: Long
  val row_id: Long
  val event: String
  val author: String
  val timestamp: Option[DateTime]

  override def getJson: JsonObject = {
    Json.obj(
      "revision" -> revision,
      "row_id" -> row_id,
      "event" -> event,
      "author" -> author,
      "timestamp" -> optionToString(timestamp)
    )
  }
}

case class RowCreatedHistory(
    override val revision: Long,
    override val row_id: Long,
    override val event: String,
    override val author: String,
    override val timestamp: Option[DateTime]
) extends CellHistory {

  override def getJson: JsonObject = {
    super.getJson
  }
}

case class CellChangedHistory(
    override val revision: Long,
    override val row_id: Long,
    column_id: Long,
    override val event: String,
    override val author: String,
    override val timestamp: Option[DateTime],
    historyType: String,
    languageType: LanguageType,
    value: JsonObject
) extends CellHistory {

  override def getJson: JsonObject = {

    super.getJson
      .mergeIn(
        Json.obj(
          "column_id" -> column_id,
          "type" -> Try(TableauxDbType(historyType).toString).getOrElse(null),
          "languageType" -> languageType.toString,
          "value" -> Json.emptyObj()
        )
      )
      .mergeIn(value)
  }
}

case class SeqCellHistory(rows: Seq[CellHistory]) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("rows" -> (rows map (_.getJson)))
  }
}
