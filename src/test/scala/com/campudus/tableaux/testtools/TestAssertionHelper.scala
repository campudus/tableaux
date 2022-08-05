package com.campudus.tableaux.testtools

import org.vertx.scala.core.json.{JsonArray, JsonObject}

import org.junit.Assert._
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

trait JsonAssertable[T] {
  def serialize(t: T): String
}

object JsonAssertable {

  implicit object String extends JsonAssertable[String] {
    def serialize(t: String) = t
  }

  implicit object JsonObject extends JsonAssertable[JsonObject] {
    def serialize(t: JsonObject) = t.toString
  }

  implicit object JsonArray extends JsonAssertable[JsonArray] {
    def serialize(t: JsonArray) = t.toString
  }
}

trait TestAssertionHelper {

  protected def checkPartsInRandomOrder(parts: Seq[Seq[Any]], longList: Seq[Any]): Unit = {
    assertEquals(parts.map(_.length).sum, longList.length)
    val indices = for {
      part <- parts
    } yield longList.indexOfSlice(part)
    assertTrue(indices.forall(_ >= 0))
    assertEquals(indices.length, indices.distinct.length)
  }

  def assertJSONEquals[T: JsonAssertable](
      expected: T,
      actual: T,
      compareMode: JSONCompareMode = JSONCompareMode.LENIENT
  ) {
    val expectedString = implicitly[JsonAssertable[T]].serialize(expected)
    val actualString = implicitly[JsonAssertable[T]].serialize(actual)

    JSONAssert.assertEquals(expectedString, actualString, compareMode)
  }
}
