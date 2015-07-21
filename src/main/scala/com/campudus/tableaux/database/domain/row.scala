package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Row(table: Table, id: RowId, values: Seq[_]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "values" -> compatibilityGet(values))

  override def setJson: JsonObject = Json.obj("id" -> id)
}

case class RowSeq(rows: Seq[Row]) extends DomainObject {
  override def getJson: JsonObject = Json.obj("rows" -> (rows map (_.getJson)))

  override def setJson: JsonObject = Json.obj("rows" -> (rows map (_.setJson)))
}
