package com.campudus.tableaux.helper

import io.vertx.lang.scala.json.{JsonArray, JsonObject}

/**
  * Builder for constructing [[JsonObject]]s and [[JsonArray]]s from varargs.
  *
  * Deliberately does *not* offer a single-String overload like `io.vertx.lang.scala.json.Json.obj(json: String)` /
  * `.arr(json: String)`: those parse the string as JSON, which silently shadows the vararg overload for any call site
  * that passes a single plain String meant to become a one-element array/object (e.g. `Json.arr(name)`). Use
  * `new JsonObject(str)` / `new JsonArray(str)` directly when the intent really is to parse a JSON string.
  */
object Json {

  def obj(): JsonObject = new JsonObject()

  def arr(): JsonArray = new JsonArray()

  def obj(fields: (String, Any)*): JsonObject = {
    val o = new JsonObject()
    fields.foreach {
      case (key, l: Array[?]) => addToObject(o, key, listToJsArr(l.toIndexedSeq))
      case (key, l: Seq[?]) => addToObject(o, key, listToJsArr(l))
      case (key, value) => addToObject(o, key, value)
    }
    o
  }

  def arr(fields: Any*): JsonArray = {
    val a = new JsonArray()
    fields.foreach {
      case array: Array[?] => addToArray(a, listToJsArr(array.toIndexedSeq))
      case seq: Seq[?] => addToArray(a, listToJsArr(seq))
      case f => addToArray(a, f)
    }
    a
  }

  private def listToJsArr(a: Seq[?]) = arr(a*)

  private def addToArray(a: JsonArray, fieldValue: Any): JsonArray = {
    if (fieldValue == null) a.addNull() else a.add(fieldValue)
  }

  private def addToObject(o: JsonObject, fieldName: String, fieldValue: Any): JsonObject = {
    if (fieldValue == null) o.putNull(fieldName) else o.put(fieldName, fieldValue)
  }
}
