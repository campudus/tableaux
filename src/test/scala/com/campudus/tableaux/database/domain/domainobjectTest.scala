package com.campudus.tableaux.database.domain

import org.vertx.scala.core.json.{Json, JsonObject}

import org.junit.Test
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

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

    val innerRowLevelAnnotations1 = RowLevelAnnotations(finalFlag = true, archivedFlag = false)
    val innerRowLevelAnnotations2 = RowLevelAnnotations(finalFlag = false, archivedFlag = true)
    val innerRowLevelAnnotations3 = RowLevelAnnotations(finalFlag = false, archivedFlag = false)
    val rowLevelAnnotations = RowLevelAnnotations(finalFlag = true, archivedFlag = true)
    val innerCell1 = Cell(null, 1, 1, innerRowLevelAnnotations1)
    val innerCell2 = Cell(null, 1, 2, innerRowLevelAnnotations2)
    val innerCell3 = Cell(null, 1, 3, innerRowLevelAnnotations3)
    val cell = Cell(null, 1, Seq(innerCell1, innerCell2, innerCell3), rowLevelAnnotations)

    val actual = DomainObject.compatibilityGet(cell)

    JSONAssert
      .assertEquals(
        s"""{"value": [{"value":1, "final":true}, {"value":2, "archived":true}, {"value":3}], "final":true, "archived":true}""",
        actual.toString,
        JSONCompareMode.STRICT
      )
  }
}
