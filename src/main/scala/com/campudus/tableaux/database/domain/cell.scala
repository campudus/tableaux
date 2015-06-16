package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A) extends DomainObject {
  def getJson: JsonObject = {
    val v = value match {
      case s: Seq[_] => s map { case d: DomainObject => d.getJson }
      case d: DomainObject => d.getJson
      case _ => value
    }

    Json.obj("rows" -> Json.arr(Json.obj("value" -> v)))
  }

  def setJson: JsonObject = Json.obj()
}