package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.testtools.TestAssertionHelper

import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._

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

    val originColumnsJson = Json.fromObjectString(jString)
    val json = originColumnsJson.getJsonArray("originColumns", Json.emptyArr())
    val originColumns = CreateOriginColumns.parseJson(json)

    assertEquals(Map(1 -> 3, 2 -> 2, 3 -> 1), originColumns.tableId2ColumnId)
  }

  @Test
  def parseOriginColumns_emptyArray_error(): Unit = {
    val json = Json.emptyArr()
    assertThrows[IllegalArgumentException] {
      CreateOriginColumns.parseJson(json)
    }
  }

  @Test
  def parseOriginColumns_missingArray_error(): Unit = {
    assertThrows[IllegalArgumentException] {
      CreateOriginColumns.parseJson()
    }
  }
}
