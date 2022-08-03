package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.{EmptyReturn, GetReturn, ReturnType}
import org.vertx.scala.core.json._

import scala.collection.mutable
import scala.util.Try
import io.vertx.scala.ext.web.RoutingContext

object DomainObject extends DomainObject {
  override def getJson(implicit routingContext: RoutingContext): JsonObject = Json.emptyObj()
}

trait DomainObjectHelper {

  // def compatibilityGet[A]: (A => Any) = compatibility(GetReturn)(_)
  def compatibilityGet[A](value: A)(implicit routingContext: RoutingContext): A => Any = compatibility(GetReturn)(_)

  private def compatibility[A](returnType: ReturnType)(value: A)(implicit routingContext: RoutingContext): Any = {
    value match {
      case Some(a) => compatibility(returnType)(a)
      case None => None.orNull
      case s: Seq[_] => compatibilitySeq(returnType)(s)
      case d: DomainObject => d.toJson(returnType)
      case _ => value
    }
  }

  private def compatibilitySeq[A](returnType: ReturnType)(values: Seq[A])(
      implicit routingContext: RoutingContext
  ): java.util.List[_] = {
    import scala.collection.JavaConverters._

    values
      .map({
        case s: Seq[_] => compatibilitySeq(returnType)(s)
        case d: DomainObject => d.toJson(returnType)
        case e => compatibility(returnType)(e)
      })
      .asJava
  }

  def optionToString[A](option: Option[A]): String = {
    option.map(_.toString).orNull
  }
}

trait DomainObject extends DomainObjectHelper {

  def getJson(implicit routingContext: RoutingContext): JsonObject

  /**
    * Returns an empty JsonObject. It's used as response for all requests which don't need a response body.
    *
    * @return
    *   empty JsonObject
    */
  final def emptyJson: JsonObject = Json.obj()

  /**
    * @param returnType
    *   get, set or empty
    * @return
    */
  final def toJson(returnType: ReturnType)(implicit routingContext: RoutingContext): JsonObject = returnType match {
    case GetReturn => getJson
    case EmptyReturn => emptyJson
  }

  /**
    * Uses getJson to encode DomainObject as String
    *
    * @return
    *   String representation of DomainObject
    */
  def toString(implicit routingContext: RoutingContext): String = getJson.encode()
}

case class EmptyObject() extends DomainObject {

  override def getJson(implicit routingContext: RoutingContext): JsonObject = Json.emptyObj()
}

case class PlainDomainObject(json: JsonObject) extends DomainObject {

  override def getJson(implicit routingContext: RoutingContext): JsonObject = json
}

object MultiLanguageValue {

  def apply[A](values: Option[JsonObject]): MultiLanguageValue[A] = {
    if (values.isDefined) {
      MultiLanguageValue[A](values.get)
    } else {
      MultiLanguageValue.empty[A]()
    }
  }

  def apply[A](values: (String, A)*): MultiLanguageValue[A] = {
    MultiLanguageValue[A](values.toMap)
  }

  /**
    * Generates MultiLanguageValue based on JSON
    */
  def apply[A](obj: JsonObject): MultiLanguageValue[A] = {
    import scala.collection.JavaConverters._
    val fields: Map[String, A] =
      obj.fieldNames().asScala.map(name => name -> obj.getValue(name).asInstanceOf[A])(collection.breakOut)

    MultiLanguageValue[A](fields)
  }

  def empty[A](): MultiLanguageValue[A] = {
    MultiLanguageValue[A](Map.empty[String, A])
  }

  /**
    * Map Map(column -> Map(langtag -> value)) to Map(langtag -> Map(column -> value))
    */
  def merge(map: Map[String, Map[String, Any]]): Map[String, Map[String, Any]] = {
    val result = mutable.Map.empty[String, mutable.Map[String, Any]]

    map.foreach({
      case (column, values) =>
        values.foreach({
          case (langtag, value) =>
            val columnsValueMap = result.get(langtag)

            if (columnsValueMap.isDefined) {
              columnsValueMap.get.put(column, value)
            } else {
              result.put(langtag, mutable.Map.empty[String, Any])
              result(langtag).put(column, value)
            }
        })
    })

    // convert to immutable map
    result
      .map({
        case (langtag, columnsValueMap) =>
          (langtag, columnsValueMap.toMap)
      })
      .toMap
  }

  def fromString[A](str: String): MultiLanguageValue[A] = {
    MultiLanguageValue[A](Try(Json.fromObjectString(str)).toOption)
  }
}

/**
  * Used for multi-language values in a DomainObject
  */
case class MultiLanguageValue[A](values: Map[String, A]) extends DomainObject {

  override def getJson(implicit routingContext: RoutingContext): JsonObject = {
    values.foldLeft(Json.emptyObj()) {
      case (obj, (langtag, value)) =>
        obj.mergeIn(Json.obj(langtag -> value))
    }
  }

  def langtags(implicit routingContext: RoutingContext): Seq[String] = values.keys.toSeq

  def get(langtag: String)(implicit routingContext: RoutingContext): Option[A] = values.get(langtag)

  def size(implicit routingContext: RoutingContext): Int = values.size

  def add(langtag: String, value: A)(implicit routingContext: RoutingContext): MultiLanguageValue[A] = {
    val newValues = values + (langtag -> value)
    MultiLanguageValue[A](newValues)
  }
}
