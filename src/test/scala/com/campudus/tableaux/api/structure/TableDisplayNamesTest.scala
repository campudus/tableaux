package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class TableDisplayNamesTest extends TableauxTestBase {

  @Test
  def createRegularTableWithoutDisplayName(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable"
      )
      val expectedTableJson = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB"))

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertEquals(expectedTableJson, tablePost)
        assertEquals(expectedTableJson, tableGet)
      }
    }
  }

  @Test
  def createRegularTableWithMultipleDisplayNames(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "hidden" -> false,
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table"),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB")
      )

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertEquals(expectedTableJson, tablePost)
        assertEquals(expectedTableJson, tableGet)
      }
    }
  }

  @Test
  def createRegularTableWithSingleDisplayName(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle")
      )
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "hidden" -> false,
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle"),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB")
      )

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        tableGet <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertEquals(expectedTableJson, tablePost)
        assertEquals(expectedTableJson, tableGet)
      }
    }
  }

  @Test
  def updateTableNameDoesNotChangeDisplayNames(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val updateTableJson = Json.obj("name" -> "sametable")
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sametable",
        "hidden" -> false,
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table"),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB")
      )

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
  }

  @Test
  def updateTableNameDoesNotAddDisplayNames(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj("name" -> "sometable")
      val updateTableJson = Json.obj("name" -> "sametable")
      val expectedTableJson = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sametable",
        "hidden" -> false,
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB"))

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
  }

  @Test
  def updateTableAndAddDisplayNames(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj("name" -> "sometable")
      val updateTableJson = Json.obj(
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val baseExpected = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "hidden" -> false,
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB"))
      val expectedAfterCreate = baseExpected.copy().mergeIn(Json.obj("displayName" -> Json.obj()))
      val expectedAfterUpdate = baseExpected
        .copy()
        .mergeIn(Json.obj("displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")))

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        table1 <- sendRequest("GET", s"/tables/$tableId")
        tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
        table2 <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertEquals(expectedAfterCreate, tablePost)
        assertEquals(expectedAfterCreate, table1)
        assertEquals(expectedAfterUpdate, tableUpdate)
        assertEquals(expectedAfterUpdate, table2)
      }
    }
  }

  @Test
  def updateDisplayNames(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val updateTableJson = Json.obj(
        "displayName" -> Json.obj("de-DE" -> "Immer noch eine Tabelle")
      )
      val baseExpected = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "hidden" -> false,
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB"))
      val expectedAfterCreate = baseExpected
        .copy()
        .mergeIn(Json.obj("displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")))
      val expectedAfterUpdate = baseExpected
        .copy()
        .mergeIn(Json.obj("displayName" -> Json.obj("de-DE" -> "Immer noch eine Tabelle", "en-GB" -> "Some table")))

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        table1 <- sendRequest("GET", s"/tables/$tableId")
        tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
        table2 <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertEquals(expectedAfterCreate, tablePost)
        assertEquals(expectedAfterCreate, table1)
        assertEquals(expectedAfterUpdate, tableUpdate)
        assertEquals(expectedAfterUpdate, table2)
      }
    }
  }

  @Test
  def deleteDisplayName(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val updateTableJson = Json.obj(
        "displayName" -> Json.obj("de-DE" -> null)
      )
      val baseExpected = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "hidden" -> false,
        "description" -> Json.obj(),
        "langtags" -> Json.arr("de-DE", "en-GB"))
      val expectedAfterCreate = baseExpected
        .copy()
        .mergeIn(Json.obj("displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")))
      val expectedAfterUpdate = baseExpected.copy()
        .mergeIn(Json.obj("displayName" -> Json.obj("en-GB" -> "Some table")))

      for {
        tablePost <- sendRequest("POST", "/tables", createTableJson)
        tableId = tablePost.getLong("id").toLong
        table1 <- sendRequest("GET", s"/tables/$tableId")
        tableUpdate <- sendRequest("POST", s"/tables/$tableId", updateTableJson)
        table2 <- sendRequest("GET", s"/tables/$tableId")
      } yield {
        assertEquals(expectedAfterCreate, tablePost)
        assertEquals(expectedAfterCreate, table1)
        assertEquals(expectedAfterUpdate, tableUpdate)
        assertEquals(expectedAfterUpdate, table2)
      }
    }
  }

}
