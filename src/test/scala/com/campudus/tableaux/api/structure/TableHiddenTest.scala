package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class TableHiddenTest extends TableauxTestBase {

  @Test
  def createRegularTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test", "hidden" -> false)
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
  def createRegularTableWithoutHiddenFlag(implicit c: TestContext): Unit = okTest {
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
  def createHiddenTable(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj("name" -> "Hidden table test", "hidden" -> true)
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Hidden table test",
        "hidden" -> true,
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
  }

  @Test
  def updateTableNameDoesNotChangeHiddenFlagFromTrue(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj("name" -> "Hidden table test", "hidden" -> true)
      val updateTableJson = Json.obj("name" -> "Still hidden table test")
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Still hidden table test",
        "hidden" -> true,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB")
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
  def updateTableNameDoesNotChangeHiddenFlagFromFalse(implicit c: TestContext): Unit = {
    okTest {
      val createTableJson = Json.obj("name" -> "Regular table test", "hidden" -> false)
      val updateTableJson = Json.obj("name" -> "Still regular table test")
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "Still regular table test",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB")
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
  def updateHiddenFlagTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Some table test", "hidden" -> false)
    val updateTableJson = Json.obj("hidden" -> true)
    val expectedTableJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "name" -> "Some table test",
      "displayName" -> Json.obj(),
      "description" -> Json.obj(),
      "langtags" -> Json.arr("de-DE", "en-GB")
    )
    val expectedTableJson1 = expectedTableJson.copy().mergeIn(Json.obj("hidden" -> false))
    val expectedTableJson2 = expectedTableJson.copy().mergeIn(Json.obj("hidden" -> true))

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
