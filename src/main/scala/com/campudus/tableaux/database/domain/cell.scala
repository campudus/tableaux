package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

// TODO make A dependent of ColumnType.ScalaType somehow
case class Cell[A](column: ColumnType, rowId: RowId, value: A) extends DomainObject {
  override def getJson: JsonObject = Json.obj("value" -> compatibilityGet(value))
}