package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Cell[A](column: ColumnType[A], rowId: RowId, value: A) extends DomainObject {
  override def getJson: JsonObject = Json.obj("value" -> compatibilityGet(value))

  override def setJson: JsonObject = Json.obj()
}