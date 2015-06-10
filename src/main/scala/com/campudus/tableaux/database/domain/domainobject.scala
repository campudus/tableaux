package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.{EmptyReturn, SetReturn, GetReturn, ReturnType}
import org.vertx.scala.core.json._

trait DomainObjectHelper {
  def optionToString[A](option: Option[A]): String = {
    option.map(_.toString).orNull
  }
}

trait DomainObject extends DomainObjectHelper {
  def getJson: JsonObject

  def setJson: JsonObject

  /**
   * Returns an empty JsonObject. It's used
   * as response for all requests which don't
   * need a response body.
   *
   * @return empty JsonObject
   */
  final def emptyJson: JsonObject = Json.obj()

  /**
   *
   * @param returnType get, set or empty {@see ReturnType}
   * @return
   */
  def toJson(returnType: ReturnType): JsonObject = returnType match {
    case GetReturn => getJson
    case SetReturn => setJson
    case EmptyReturn => emptyJson
  }
}

case class EmptyObject() extends DomainObject {
  def getJson: JsonObject = Json.obj()

  def setJson: JsonObject = Json.obj()
}