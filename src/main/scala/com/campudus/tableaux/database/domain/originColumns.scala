package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.domain.DisplayInfos._
import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

sealed trait CreateOriginColumns {
  val fieldName = "originColumns"
  val tableToColumn: Map[TableId, ColumnId]

  def getJson: JsonObject = {
    Json.obj(fieldName -> Json.obj(tableToColumn.map { case (tableId, columnId) =>
      tableId.toString -> Json.obj("id" -> columnId)
    }.toSeq: _*))
  }
}

object CreateOriginColumns {
  val fieldName = "originColumns"

  def getFieldNames(json: JsonObject): Seq[String] = {
    import scala.collection.JavaConverters._

    if (json.fieldNames().size() > 0) {
      json.fieldNames().asScala.toSeq
    } else {
      Seq.empty
    }
  }

  def apply(_tableToColumn: Map[TableId, ColumnId]): CreateOriginColumns = {
    new CreateOriginColumns {
      override val tableToColumn: Map[TableId, ColumnId] = _tableToColumn
    }
  }

  def unapply(originColumns: CreateOriginColumns): Option[Map[TableId, ColumnId]] = {
    Some(originColumns.tableToColumn)
  }

  def fromJson(json: JsonObject): CreateOriginColumns = {
    val jsonPart = json.getJsonObject(fieldName, Json.obj())

    val tableToColumn: Map[TableId, ColumnId] = getFieldNames(jsonPart).map { tableId =>
      val columnId = jsonPart.getJsonObject(tableId).getLong("id").toLong
      (tableId.toLong, columnId)
    }.toMap

    if (tableToColumn.isEmpty) {
      throw new IllegalArgumentException(s"Origin columns must not be empty in $json")
    }

    CreateOriginColumns(tableToColumn)
  }
}
