package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A) extends DomainObject {
  override def getJson: JsonObject = {
    val v = compatibilityGet(value)
    Json.obj("rows" -> Json.arr(Json.obj("value" -> v)))
  }

  override def setJson: JsonObject = Json.obj()
}