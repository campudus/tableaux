package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

import scala.collection.JavaConverters._

sealed trait OriginColumnsBase {
  val tableId2ColumnId: Map[TableId, ColumnId]
}

object OriginColumnsBase {

  def parseJson(json: JsonArray): Map[TableId, ColumnId] = {
    val tableId2ColumnId = (json.asScala).map(json => {
      val singleMapping = json.asInstanceOf[JsonObject]
      val tableId: TableId = singleMapping.getLong("tableId")
      val columnId: ColumnId = singleMapping.getLong("columnId")
      (tableId, columnId)
    }).toMap

    if (tableId2ColumnId.isEmpty) {
      throw new IllegalArgumentException(s"Origin columns must not be empty in $json")
    }

    tableId2ColumnId
  }
}

case class OriginColumns(
    tableId2ColumnId: Map[TableId, ColumnId],
    tableId2Column: Map[TableId, ColumnType[_]] = Map() // placeholder
) extends OriginColumnsBase {

  def getJson: JsonArray = {
    Json.arr(tableId2Column.map { case (tableId, column) =>
      Json.obj("tableId" -> tableId, "column" -> column.getJson)
    }.toSeq: _*)
  }
}

object OriginColumns {
  val fieldName = "originColumns"

  def parseJson(json: JsonArray = Json.emptyArr()): OriginColumns = {
    val tableToColumn = OriginColumnsBase.parseJson(json)
    OriginColumns(tableToColumn)
  }
}

case class CreateOriginColumns(tableId2ColumnId: Map[TableId, ColumnId]) extends OriginColumnsBase {}

object CreateOriginColumns {
  val fieldName = OriginColumns.fieldName

  def parseJson(json: JsonArray = Json.emptyArr()): CreateOriginColumns = {
    val tableToColumn = OriginColumnsBase.parseJson(json)
    CreateOriginColumns(tableToColumn)
  }
}
