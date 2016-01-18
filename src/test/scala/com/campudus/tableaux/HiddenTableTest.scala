package com.campudus.tableaux

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
    val expectedPostJson = Json.obj("status" -> "ok", "id" -> 1)
    val expectedGetJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Regular table test", "hidden" -> false)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedPostJson, tablePost)
      assertEquals(expectedGetJson, tableGet)
    }
  }

  @Test
  def createRegularTableWithoutHiddenFlag(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test")
    val expectedPostJson = Json.obj("status" -> "ok", "id" -> 1)
    val expectedGetJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Regular table test", "hidden" -> false)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedPostJson, tablePost)
      assertEquals(expectedGetJson, tableGet)
    }
  }

  @Test
  def createHiddenTable(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Hidden table test", "hidden" -> true)
    val expectedPostJson = Json.obj("status" -> "ok", "id" -> 1)
    val expectedGetJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Hidden table test", "hidden" -> true)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableGet <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedPostJson, tablePost)
      assertEquals(expectedGetJson, tableGet)
    }
  }

  @Test
  def updateTableNameDoesNotChangeHiddenFlagFromTrue(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Hidden table test", "hidden" -> true)
    val updateTableJson = Json.obj("name" -> "Still hidden table test")
    val expectedGetJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Still hidden table test", "hidden" -> true)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
      table <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedGetJson, table)
    }
  }

  @Test
  def updateTableNameDoesNotChangeHiddenFlagFromFalse(implicit c: TestContext): Unit = okTest {
    val createTableJson = Json.obj("name" -> "Regular table test", "hidden" -> false)
    val updateTableJson = Json.obj("name" -> "Still regular table test")
    val expectedGetJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "Still regular table test", "hidden" -> false)

    for {
      tablePost <- sendRequest("POST", "/tables", createTableJson)
      tableId = tablePost.getLong("id").toLong
      tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
      table <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertEquals(expectedGetJson, table)
    }
  }

}
