package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.{EmptyReturn, GetReturn, ReturnType, SetReturn}
import org.vertx.scala.core.json._

trait DomainObjectHelper {

  def compatibilitySet[A]: A => Any = compatibility(SetReturn)(_)

  def compatibilityGet[A]: A => Any = compatibility(GetReturn)(_)

  private def compatibility[A](returnType: ReturnType)(value: A): Any = {
    value match {
      case s: Seq[_] => compatibilitySeq(returnType)(s)
      case d: DomainObject => d.toJson(returnType)
      case _ => value
    }
  }

  private def compatibilitySeq[A](returnType: ReturnType)(values: Seq[A]): Seq[_] = {
    values map {
      case s: Seq[_] => compatibilitySeq(returnType)(s)
      case d: DomainObject => d.toJson(returnType)
      case e => e
    }
  }

  def optionToString[A](option: Option[A]): String = {
    option.map(_.toString).orNull
  }
}

trait DomainObject extends DomainObjectHelper {
  def getJson: JsonObject

  def setJson: JsonObject = getJson

  /**
   * Returns an empty JsonObject. It's used
   * as response for all requests which don't
   * need a response body.
   *
   * @return empty JsonObject
   */
  final def emptyJson: JsonObject = Json.obj()

  /**
   * @param returnType get, set or empty
   * @return
   */
  final def toJson(returnType: ReturnType): JsonObject = returnType match {
    case GetReturn => getJson
    case SetReturn => setJson
    case EmptyReturn => emptyJson
  }

  /**
   * Uses getJson to encode DomainObject as String
   * @return String representation of DomainObject
   */
  final override def toString: String = getJson.encode()
}

case class EmptyObject() extends DomainObject {
  override def getJson: JsonObject = Json.emptyObj()
}