package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.{EmptyReturn, GetReturn, ReturnType}
import org.vertx.scala.core.json._

import scala.collection.mutable

trait DomainObjectHelper {

  def compatibilityGet[A]: A => Any = compatibility(GetReturn)(_)

  private def compatibility[A](returnType: ReturnType)(value: A): Any = {
    value match {
      case Some(a) => compatibility(returnType)(a)
      case None => null
      case s: Seq[_] => compatibilitySeq(returnType)(s)
      case d: DomainObject => d.toJson(returnType)
      case _ => value
    }
  }

  private def compatibilitySeq[A](returnType: ReturnType)(values: Seq[A]): Seq[_] = {
    values map {
      case s: Seq[_] => compatibilitySeq(returnType)(s)
      case d: DomainObject => d.toJson(returnType)
      case e => compatibility(returnType)(e)
    }
  }

  def optionToString[A](option: Option[A]): String = {
    option.map(_.toString).orNull
  }
}

trait DomainObject extends DomainObjectHelper {
  def getJson: JsonObject

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
    case EmptyReturn => emptyJson
  }

  /**
    * Uses getJson to encode DomainObject as String
    *
    * @return String representation of DomainObject
    */
  override def toString: String = getJson.encode()
}

case class EmptyObject() extends DomainObject {
  override def getJson: JsonObject = Json.emptyObj()
}

case class PlainDomainObject(json: JsonObject) extends DomainObject {
  override def getJson: JsonObject = json
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
    val fields: Map[String, A] = obj.fieldNames().asScala.map(name => name -> obj.getValue(name).asInstanceOf[A])(collection.breakOut)

    MultiLanguageValue[A](fields)
  }

  def empty[A](): MultiLanguageValue[A] = {
    MultiLanguageValue[A](Map.empty[String, A])
  }

  /**
    * Map Map(column -> Map(langtag -> value))
    * to Map(langtag -> Map(column -> value))
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
              result.get(langtag).get.put(column, value)
            }
        })
    })

    // convert to immutable map
    result.map({
      case (langtag, columnsValueMap) =>
        (langtag, columnsValueMap.toMap)
    }).toMap
  }
}

/**
  * Used for multi-language values in a DomainObject
  */
case class MultiLanguageValue[A](values: Map[String, A]) extends DomainObject {
  override def getJson: JsonObject = {
    values.foldLeft(Json.emptyObj()) {
      case (obj, (langtag, value)) =>
        obj.mergeIn(Json.obj(langtag -> value))
    }
  }

  def langtags: Seq[String] = values.keys.toSeq

  def get(langtag: String): Option[A] = values.get(langtag)

  def size = values.size

  def add(langtag: String, value: A): MultiLanguageValue[A] = {
    val newValues = values + (langtag -> value)
    MultiLanguageValue[A](newValues)
  }
}