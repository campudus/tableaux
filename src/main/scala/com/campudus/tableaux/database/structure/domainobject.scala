package com.campudus.tableaux.database.structure

import org.vertx.scala.core.json._

trait DomainObject {
  def getJson: JsonObject

  def setJson: JsonObject

  def emptyJson: JsonObject = Json.obj()

  def toJson(reType: ReturnType): JsonObject = reType match {
    case GetReturn => getJson
    case SetReturn => setJson
    case EmptyReturn => emptyJson
  }
}

case class EmptyObject() extends DomainObject {
  def getJson: JsonObject = Json.obj()

  def setJson: JsonObject = Json.obj()
}