package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.TestAssertionHelper

import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test

class CreateOriginColumnsTest extends TestAssertionHelper {

  @Test
  def parseCreateOriginColumnsObject(): Unit = {
    val jString = ("""
                      |{
                      |  "originColumns": {
                      |    "1": { "id": 3 },
                      |    "2": { "id": 2 },
                      |    "3": { "id": 1 }
                      |  }
                      |}""".stripMargin)

    val json = Json.fromObjectString(jString)
    val createOriginColumns = CreateOriginColumns.fromJson(json)

    assertEquals(Map(1 -> 3, 2 -> 2, 3 -> 1), createOriginColumns.tableToColumn)
    assertJSONEquals(json, createOriginColumns.getJson)
  }
}
