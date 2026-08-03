package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.testtools.TestAssertionHelper

import io.vertx.lang.scala.json.JsonObject

import org.junit.Assert._
import org.junit.Test

class CreateOriginColumnsTest extends TestAssertionHelper {

  @Test
  def parseOriginColumns_validArray_ok(): Unit = {
    val jString = ("""
                      |{
                      |  "originColumns": [
                      |    { "tableId": 1, "columnId": 3 },
                      |    { "tableId": 2, "columnId": 2 },
                      |    { "tableId": 3, "columnId": 1 }
                      |  ]
                      |}""".stripMargin)

    val originColumnsJson = new JsonObject(jString)
    val json = originColumnsJson.getJsonArray("originColumns", Json.arr())
    val originColumns = CreateOriginColumns.parseJson(json)

    assertEquals(Map(1 -> 3, 2 -> 2, 3 -> 1), originColumns.tableId2ColumnId)
  }

  @Test
  def parseOriginColumns_emptyArray_error(): Unit = {
    val json = Json.arr()
    assertThrows(classOf[IllegalArgumentException], () => CreateOriginColumns.parseJson(json))
  }

  @Test
  def parseOriginColumns_missingArray_error(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => CreateOriginColumns.parseJson())
  }
}
