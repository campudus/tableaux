package org.vertx.scala.core.json

class ScalaJsonArray(list: java.util.List[_]) extends JsonArray(list) {
  def get[A](pos: Int): A = super.getValue(pos).asInstanceOf[A]
}

class ScalaJsonObject(map: java.util.Map[String, Object]) extends JsonObject(map) {
  def containsField(field: String): Boolean = super.containsKey(field)

  def getArray(field: String): JsonArray = super.getJsonArray(field)

  def getObject(field: String): JsonObject = super.getJsonObject(field)

  def getNumber(field: String): Number = super.getValue(field).asInstanceOf[Number]

  def removeField[A](field: String): A = super.remove(field).asInstanceOf[A]
}

trait JsonCompatible {
  implicit def mapToCompatibleJsonArray(arr: JsonArray): ScalaJsonArray = new ScalaJsonArray(arr.getList)

  implicit def mapToCompatibleJsonObject(obj: JsonObject): ScalaJsonObject = new ScalaJsonObject(obj.getMap)
}