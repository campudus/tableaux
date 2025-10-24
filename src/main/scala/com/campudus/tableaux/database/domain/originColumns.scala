package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain.DisplayInfos._
import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

import scala.collection.JavaConverters._

sealed trait OriginColumns {
  val tableToColumn: Map[TableId, ColumnId]

  def getJson: JsonObject = {
    val arr = Json.arr(tableToColumn.map { case (tableId, columnId) =>
      Json.obj("tableId" -> tableId, "columnId" -> columnId)
    }.toSeq: _*)
    Json.obj(OriginColumns.fieldName -> arr)
  }
}

object OriginColumns {
  val fieldName = "originColumns"

  def getFieldNames(json: JsonObject): Seq[String] = {
    import scala.collection.JavaConverters._

    if (json.fieldNames().size() > 0) {
      json.fieldNames().asScala.toSeq
    } else {
      Seq.empty
    }
  }

  def apply(_tableToColumn: Map[TableId, ColumnId]): OriginColumns = {
    new OriginColumns {
      override val tableToColumn: Map[TableId, ColumnId] = _tableToColumn
    }
  }

  def unapply(originColumns: OriginColumns): Option[Map[TableId, ColumnId]] = {
    Some(originColumns.tableToColumn)
  }

  def fromJson(json: JsonArray = Json.emptyArr()): OriginColumns = {
    val tableToColumn: Map[TableId, ColumnId] = (json.asScala).map(json => {
      val singleMapping = json.asInstanceOf[JsonObject]
      val tableId: TableId = singleMapping.getLong("tableId")
      val columnId: ColumnId = singleMapping.getLong("columnId")
      (tableId, columnId)
    }).toMap

    if (tableToColumn.isEmpty) {
      throw new IllegalArgumentException(s"Origin columns must not be empty in $json")
    }

    OriginColumns(tableToColumn)
  }
}
