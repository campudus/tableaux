package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.TestAssertionHelper

import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions._

class CreateOriginColumnsTest extends TestAssertionHelper {

  @Test
  def parseCreateOriginColumns_validObject_ok(): Unit = {
    val jString = ("""
                      |{
                      |  "originColumns": {
                      |    "1": { "id": 3 },
                      |    "2": { "id": 2 },
                      |    "3": { "id": 1 }
                      |  }
                      |}""".stripMargin)

    val originColumnsJson = Json.fromObjectString(jString)
    val json = originColumnsJson.getJsonObject("originColumns", Json.emptyObj())
    val createOriginColumns = CreateOriginColumns.fromJson(json)

    assertEquals(Map(1 -> 3, 2 -> 2, 3 -> 1), createOriginColumns.tableToColumn)
    assertJSONEquals(originColumnsJson, createOriginColumns.getJson)
  }

  @Test
  def parseCreateOriginColumns_emptyObject_error(): Unit = {
    val json = Json.emptyObj()
    assertThrows[IllegalArgumentException] {
      CreateOriginColumns.fromJson(json)
    }
  }

  @Test
  def parseCreateOriginColumns_missingObject_error(): Unit = {
    val json = Json.emptyObj()
    assertThrows[IllegalArgumentException] {
      CreateOriginColumns.fromJson(json)
    }
  }
}
