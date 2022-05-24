package com.campudus.tableaux.database.domain

import org.junit.Test
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}
import org.vertx.scala.core.json.{Json, JsonObject}

class DomainObjectTest {

  private def wrapIntoJsonObj(value: Any): JsonObject = Json.obj("value" -> value)

  @Test
  def simpleInteger(): Unit = {
    val actual = wrapIntoJsonObj(DomainObject.compatibilityGet(1))
    JSONAssert.assertEquals("{value: 1}", actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def simpleString(): Unit = {
    val actual = wrapIntoJsonObj(DomainObject.compatibilityGet("any String"))
    JSONAssert.assertEquals(s"""{value: "any String"}""", actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def simpleIntegerSeq(): Unit = {
    val actual = wrapIntoJsonObj(DomainObject.compatibilityGet(Seq(1, 2, 42, 1337)))
    JSONAssert.assertEquals(s"""{value: [1, 2, 42, 1337]}""", actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def mixedSeq(): Unit = {
    val actual = wrapIntoJsonObj(DomainObject.compatibilityGet(Seq(42, "and a string", 1337)))
    JSONAssert.assertEquals(s"""{value: [42, "and a string", 1337]}""", actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def nestedSeq(): Unit = {
    val actual = wrapIntoJsonObj(DomainObject.compatibilityGet(Seq(42, Seq(1, 2, 3), 1337)))
    JSONAssert.assertEquals(s"""{value: [42, [1, 2, 3], 1337]}""", actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def nestedCells(): Unit = {

    val innerCell1 = Cell(null, 1, 1, Json.obj())
    val innerCell2 = Cell(null, 1, 2, Json.obj())
    val innerCell3 = Cell(null, 1, 3, Json.obj())
    val cell = Cell(null, 1, Seq(innerCell1, innerCell2, innerCell3), Json.obj())

    val actual = DomainObject.compatibilityGet(cell)

    JSONAssert
      .assertEquals(s"""{"value": [{"value": 1}, {"value": 2}, {"value": 3}]}""",
                    actual.toString,
                    JSONCompareMode.LENIENT)
  }
}
