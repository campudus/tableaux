package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Table(id: TableId, name: String, hidden: Boolean) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "name" -> name, "hidden" -> hidden)

  override def setJson: JsonObject = Json.obj("id" -> id)
}

case class TableSeq(tables: Seq[Table]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("tables" -> compatibilityGet(tables))

  override def setJson: JsonObject = Json.obj("tables" -> compatibilitySet(tables))
}

case class CompleteTable(table: Table, columns: Seq[ColumnType[_]], rowList: RowSeq) extends DomainObject {
  override def getJson: JsonObject = table.getJson.mergeIn(Json.obj("columns" -> columns.map {_.getJson})).mergeIn(rowList.getJson)

  override def setJson: JsonObject = table.setJson.mergeIn(Json.obj("columns" -> columns.map {_.setJson})).mergeIn(rowList.setJson)
}
