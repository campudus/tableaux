/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.vertx.scala.core.json

import org.vertx.scala.core

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Cannot find add operations for type ${T}")
trait JsonElemOps[T] {

  def addToObj(o: core.json.JsonObject, key: String, v: T): core.json.JsonObject

  def addToArr(a: core.json.JsonArray, v: T): core.json.JsonArray
}

object JsonElemOps {

  implicit object JsonStringElem extends JsonElemOps[String] {

    def addToObj(o: core.json.JsonObject, key: String, v: String): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: String): core.json.JsonArray = a.add(v)
  }

  implicit object JsonIntElem extends JsonElemOps[Int] {

    def addToObj(o: core.json.JsonObject, key: String, v: Int): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: Int): core.json.JsonArray = a.add(v)
  }

  implicit object JsonBoolElem extends JsonElemOps[Boolean] {

    def addToObj(o: core.json.JsonObject, key: String, v: Boolean): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: Boolean): core.json.JsonArray = a.add(v)
  }

  implicit object JsonFloatElem extends JsonElemOps[Float] {

    def addToObj(o: core.json.JsonObject, key: String, v: Float): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: Float): core.json.JsonArray = a.add(v)
  }

  implicit object JsonJsObjectElem extends JsonElemOps[core.json.JsonObject] {

    def addToObj(o: core.json.JsonObject, key: String, v: core.json.JsonObject): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: core.json.JsonObject): core.json.JsonArray = a.add(v)
  }

  implicit object JsonJsArrayElem extends JsonElemOps[core.json.JsonArray] {

    def addToObj(o: core.json.JsonObject, key: String, v: core.json.JsonArray): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: core.json.JsonArray): core.json.JsonArray = a.add(v)
  }

  implicit object JsonBinaryElem extends JsonElemOps[Array[Byte]] {

    def addToObj(o: core.json.JsonObject, key: String, v: Array[Byte]): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: Array[Byte]): core.json.JsonArray = a.add(v)
  }

  implicit object JsonAnyElem extends JsonElemOps[Any] {

    def addToObj(o: core.json.JsonObject, key: String, v: Any): core.json.JsonObject = o.put(key, v)

    def addToArr(a: core.json.JsonArray, v: Any): core.json.JsonArray = a.add(v)
  }

}

/**
  * Helper to construct JsonObjects and JsonArrays.
  *
  * @author Edgar Chan
  * @author <a href="http://www.campudus.com/">Joern Bernhardt</a>
  */
object Json {

  /**
    * Creates a JsonArray from an encoded JSON string.
    *
    * @param json The JSON string.
    *
    * @return The decoded JsonArray.
    */
  def fromArrayString(json: String): core.json.JsonArray = new core.json.JsonArray(json)

  /**
    * Creates a JsonObject from an encoded JSON string.
    *
    * @param json The JSON string.
    *
    * @return The decoded JsonObject.
    */
  def fromObjectString(json: String): core.json.JsonObject = new core.json.JsonObject(json)

  /**
    * Creates an empty JsonArray.
    *
    * @return An empty JsonArray.
    */
  def emptyArr(): core.json.JsonArray = new core.json.JsonArray()

  /**
    * Creates an empty JsonObject.
    *
    * @return An empty JsonObject.
    */
  def emptyObj(): core.json.JsonObject = new core.json.JsonObject()

  /**
    * Constructs a JsonObject from a fieldName -> value pairs.
    *
    * @param fields The fieldName -> value pairs
    *
    * @return A JsonObject containing the name -> value pairs.
    */
  def obj(fields: (String, Any)*): core.json.JsonObject = {
    val o = new core.json.JsonObject()
    fields.foreach {
      case (key, l: Array[_]) => addToObject(o, key, listToJsArr(l))
      case (key, l: Seq[_]) => addToObject(o, key, listToJsArr(l))
      case (key, value) => addToObject(o, key, value)
    }
    o
  }

  /**
    * Creates a JsonArray from a sequence of values.
    *
    * @param fields The elements to put into the JsonArray.
    *
    * @return A JsonArray containing the provided elements.
    */
  def arr(fields: Any*): core.json.JsonArray = {
    val a = new core.json.JsonArray()
    fields.foreach {
      case array: Array[_] => addToArray(a, listToJsArr(array))
      case seq: Seq[_] => addToArray(a, listToJsArr(seq))
      case f => addToArray(a, f)
    }
    a
  }

  private def listToJsArr(a: Seq[_]) = Json.arr(a: _*)

  private def addToArray[T: JsonElemOps](a: core.json.JsonArray, fieldValue: T) = {
    if (fieldValue == null) {
      a.addNull()
    } else {
      implicitly[JsonElemOps[T]].addToArr(a, fieldValue)
    }
  }

  private def addToObject[T: JsonElemOps](o: core.json.JsonObject, fieldName: String, fieldValue: T) = {
    if (fieldValue == null) {
      o.putNull(fieldName)
    } else {
      implicitly[JsonElemOps[T]].addToObj(o, fieldName, fieldValue)
    }
  }
}
