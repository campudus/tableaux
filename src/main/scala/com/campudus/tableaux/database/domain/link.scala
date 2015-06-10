package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

case class Link[A](id: IdType, value: A) {
  def toJson: JsonObject = Json.obj("id" -> id, "value" -> value)
}
