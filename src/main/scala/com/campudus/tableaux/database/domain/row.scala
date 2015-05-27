package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

case class RowIdentifier(table: Table, id: IdType) extends DomainObject {
  def getJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))

  def setJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))
}

case class Row(table: Table, id: IdType, values: Seq[_]) extends DomainObject {
  def getJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id, "values" -> values)))

  def setJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))
}

case class RowSeq(rows: Seq[Row]) extends DomainObject {
  def getJson: JsonObject = Json.obj("rows" -> (rows map { r => Json.obj("id" -> r.id, "values" -> r.values) }))

  def setJson: JsonObject = Json.obj("rows" -> (rows map { r => Json.obj("id" -> r.id) }))
}
