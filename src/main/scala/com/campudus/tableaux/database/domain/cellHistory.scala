package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database._
import org.joda.time.DateTime
import org.vertx.scala.core.json._

object CellHistory {

  def apply(
      revision: Long,
      event: String,
      columnType: String,
      languageType: LanguageType,
      author: String,
      timestamp: Option[DateTime],
      value: JsonObject
  ): CellHistory = {
    HistoryEventType(event) match {
      case CellChangedEvent => CellChangedHistory(revision, event, author, timestamp, columnType, languageType, value)
      case RowCreatedEvent => RowCreatedHistory(revision, event, author, timestamp)
      case _ => throw new IllegalArgumentException("Invalid argument for CellHistory.apply")
    }
  }
}

sealed trait CellHistory extends DomainObject {
  val revision: Long
  val event: String
  val author: String
  val timestamp: Option[DateTime]

  override def getJson: JsonObject = {
    Json.obj(
      "revision" -> revision,
      "event" -> event,
      "author" -> author,
      "timestamp" -> optionToString(timestamp)
    )
  }
}

case class RowCreatedHistory(
    override val revision: Long,
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
    override val event: String,
    override val author: String,
    override val timestamp: Option[DateTime],
    columnType: String,
    languageType: LanguageType,
    value: JsonObject
) extends CellHistory {

  override def getJson: JsonObject = {

    super.getJson
      .mergeIn(
        Json.obj(
          "columnType" -> columnType,
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
