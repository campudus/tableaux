package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class RowIdentifier(table: Table, id: RowId) extends DomainObject {
  override def getJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))
}

case class Row(table: Table, id: RowId, values: Seq[_]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id, "values" -> compatibilityGet(values))))

  override def setJson: JsonObject = Json.obj("rows" -> Json.arr(Json.obj("id" -> id)))
}

case class RowSeq(rows: Seq[Row]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("rows" -> (rows map (_.getJson.getArray("rows").get[JsonObject](0))))

  override def setJson: JsonObject = Json.obj("rows" -> (rows map (_.setJson.getArray("rows").get[JsonObject](0))))
}
