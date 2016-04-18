package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class HiddenTableTest extends TableauxTestBase {

  @Test
  def createRegularTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test", "hidden" -> false)
    val expectedTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Regular table test", "hidden" -> false)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedTableJson, tablePost)
      assertEquals(expectedTableJson, tableGet)
    }
  }

  @Test
  def createRegularTableWithoutHiddenFlag(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test")
    val expectedTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Regular table test", "hidden" -> false)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedTableJson, tablePost)
      assertEquals(expectedTableJson, tableGet)
    }
  }

  @Test
  def createHiddenTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Hidden table test", "hidden" -> true)
    val expectedTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Hidden table test", "hidden" -> true)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedTableJson, tablePost)
      assertEquals(expectedTableJson, tableGet)
    }
  }

  @Test
  def updateTableNameDoesNotChangeHiddenFlagFromTrue(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Hidden table test", "hidden" -> true)
    val updateTableJson = Json.obj("name" -> "Still hidden table test")
    val expectedTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Still hidden table test", "hidden" -> true)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
      table <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedTableJson, tableUpdate)
      assertEquals(expectedTableJson, table)
    }
  }

  @Test
  def updateTableNameDoesNotChangeHiddenFlagFromFalse(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test", "hidden" -> false)
    val updateTableJson = Json.obj("name" -> "Still regular table test")
    val expectedTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Still regular table test", "hidden" -> false)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
      table <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedTableJson, tableUpdate)
      assertEquals(expectedTableJson, table)
    }
  }

  @Test
  def updateHiddenFlagTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Some table test", "hidden" -> false)
    val expectedFirstTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Some table test", "hidden" -> false)
    val updateTableJson = Json.obj("hidden" -> true)
    val expectedSecondTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Some table test", "hidden" -> true)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      table1 <- sendRequest("GET", s"/tables/$tableId")
      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
      table2 <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedFirstTableJson, tablePost)
      assertEquals(expectedFirstTableJson, table1)
      assertEquals(expectedSecondTableJson, tableUpdate)
      assertEquals(expectedSecondTableJson, table2)
    }
  }

}
