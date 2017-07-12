package org.vertx.scala.core.json

class ScalaJsonArray(list: java.util.List[_]) extends JsonArray(list) {

  def get[A](pos: Int): A = super.getValue(pos).asInstanceOf[A]
}

class ScalaJsonObject(map: java.util.Map[String, Object]) extends JsonObject(map) {

  def getNumber(field: String): Number = super.getValue(field).asInstanceOf[Number]
}

trait JsonCompatible {

  import scala.language.implicitConversions

  implicit def mapToCompatibleJsonArray(arr: JsonArray): ScalaJsonArray = new ScalaJsonArray(arr.getList)

  implicit def mapToCompatibleJsonObject(obj: JsonObject): ScalaJsonObject = new ScalaJsonObject(obj.getMap)
}
