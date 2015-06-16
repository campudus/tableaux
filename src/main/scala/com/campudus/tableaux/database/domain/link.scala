package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

case class Link[A](id: IdType, value: A) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "value" -> value)
  override def setJson: JsonObject = getJson
}
