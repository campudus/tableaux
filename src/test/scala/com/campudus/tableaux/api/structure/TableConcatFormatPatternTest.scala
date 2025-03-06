package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class TableConcatFormatPatternTest extends TableauxTestBase {

  @Test
  def createRegularTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test")
    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Regular table test",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertJSONEquals(expectedTableJson, tablePost)
      assertJSONEquals(expectedTableJson, tableGet)
    }
  }

  @Test
  def createRegularTableWithConcatFormatPattern(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "concatFormatPattern test", "concatFormatPattern" -> "{{1}} | {{2}}")
    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "concatFormatPattern test",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB"),
      "concatFormatPattern" -> "{{1}} | {{2}}"
    )

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertJSONEquals(expectedTableJson, tablePost)
      assertJSONEquals(expectedTableJson, tableGet)
    }
  }

  @Test
  def updateTableNameDoesNotChangeConcatFormatPattern(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj("name" -> "concatFormatPattern test", "concatFormatPattern" -> "{{1}} | {{2}}")
      val updateTableJson = Json.obj("name" -> "Still concatFormatPattern test")
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Still concatFormatPattern test",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB"),
        "concatFormatPattern" -> "{{1}} | {{2}}"
      )

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
        table <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertJSONEquals(expectedTableJson, tableUpdate)
        assertJSONEquals(expectedTableJson, table)
      }
    }
  }

  @Test
  def updateTableConcatFormatPattern(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "concatFormatPattern test", "concatFormatPattern" -> "{{1}} | {{2}}")
    val updateTableJson = Json.obj("concatFormatPattern" -> "{{1}} | {{2}} | {{3}}")
    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "concatFormatPattern test",
      "hidden" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedTableJson1 = expectedTableJson.copy().mergeIn(Json.obj("concatFormatPattern" -> "{{1}} | {{2}}"))
    val expectedTableJson2 =
      expectedTableJson.copy().mergeIn(Json.obj("concatFormatPattern" -> "{{1}} | {{2}} | {{3}}"))

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      table1 <- sendRequest("GET", s"/tables/$tableId")
      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
      table2 <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertJSONEquals(expectedTableJson1, tablePost)
      assertJSONEquals(expectedTableJson1, table1)
      assertJSONEquals(expectedTableJson2, tableUpdate)
      assertJSONEquals(expectedTableJson2, table2)
    }
  }

}
