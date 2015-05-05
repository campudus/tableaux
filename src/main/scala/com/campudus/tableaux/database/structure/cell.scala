package com.campudus.tableaux.database.structure

import com.campudus.tableaux.database.DomainObject
import com.campudus.tableaux.database.Tableaux._
import org.vertx.scala.core.json._

case class Cell[A, B <: ColumnType[A]](column: B, rowId: IdType, value: A) extends DomainObject {
  def getJson: JsonObject = {
    val v = value match {
      case link: Link[A] => link.toJson
      case _ => value
    }
    Json.obj("rows" -> Json.arr(Json.obj("value" -> v)))
  }

  def setJson: JsonObject = Json.obj()
}