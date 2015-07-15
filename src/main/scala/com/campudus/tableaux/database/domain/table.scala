package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Table(id: TableId, name: String) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "name" -> name)

  override def setJson: JsonObject = Json.obj("id" -> id)
}

case class TableSeq(tables: Seq[Table]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("tables" -> compatibilityGet(tables))

  override def setJson: JsonObject = Json.obj("tables" -> compatibilitySet(tables))
}

case class CompleteTable(table: Table, columnList: Seq[ColumnType[_]], rowList: RowSeq) extends DomainObject {
  override def getJson: JsonObject = table.getJson.mergeIn(Json.obj("columns" -> (columnList map {
    _.getJson.getArray("columns").get[JsonObject](0)
  }))).mergeIn(rowList.getJson)

  override def setJson: JsonObject = table.setJson.mergeIn(Json.obj("columns" -> (columnList map {
    _.setJson.getArray("columns").get[JsonObject](0)
  }))).mergeIn(rowList.setJson)
}
