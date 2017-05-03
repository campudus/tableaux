package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class TableDescriptionsTest extends TableauxTestBase {

  @Test
  def createRegularTableWithoutDescription(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable"
      )
      val expectedTableJson = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "hidden" -> false,
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
  def createRegularTableWithMultipleDescriptions(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj(),
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table"),
        "hidden" -> false,
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
  def createRegularTableWithSingleDescription(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "description" -> Json.obj("de-DE" -> "Eine Tabelle")
      )
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj(),
        "description" -> Json.obj("de-DE" -> "Eine Tabelle"),
        "hidden" -> false,
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
  def updateTableNameDoesNotChangeDescriptions(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val updateTableJson = Json.obj("name" -> "sametable")
      val expectedTableJson = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sametable",
        "displayName" -> Json.obj(),
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table"),
        "hidden" -> false,
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
  def updateTableNameDoesNotAddDescriptions(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj("name" -> "sometable")
      val updateTableJson = Json.obj("name" -> "sametable")
      val expectedTableJson = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sametable",
        "displayName" -> Json.obj(),
        "description" -> Json.obj(),
        "hidden" -> false,
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
  def updateTableAndAddDescriptions(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj("name" -> "sometable")
      val updateTableJson = Json.obj(
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val baseExpected = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj(),
        "hidden" -> false,
        "langtags" -> Json.arr("de-DE", "en-GB"))
      val expectedAfterCreate = baseExpected.copy().mergeIn(Json.obj("description" -> Json.obj()))
      val expectedAfterUpdate = baseExpected
        .copy()
        .mergeIn(Json.obj("description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")))

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
  def updateDescriptions(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val updateTableJson = Json.obj(
        "description" -> Json.obj("de-DE" -> "Immer noch eine Tabelle")
      )
      val baseExpected = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj(),
        "hidden" -> false,
        "langtags" -> Json.arr("de-DE", "en-GB"))
      val expectedAfterCreate = baseExpected
        .copy()
        .mergeIn(Json.obj("description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")))
      val expectedAfterUpdate = baseExpected
        .copy()
        .mergeIn(Json.obj("description" -> Json.obj("de-DE" -> "Immer noch eine Tabelle", "en-GB" -> "Some table")))

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
  def deleteDescription(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")
      )
      val updateTableJson = Json.obj(
        "description" -> Json.obj("de-DE" -> null)
      )
      val baseExpected = Json.obj("status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj(),
        "hidden" -> false,
        "langtags" -> Json.arr("de-DE", "en-GB"))
      val expectedAfterCreate = baseExpected
        .copy()
        .mergeIn(Json.obj("description" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table")))
      val expectedAfterUpdate = baseExpected.copy()
        .mergeIn(Json.obj("description" -> Json.obj("en-GB" -> "Some table")))

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
  def updateDescriptionDoesNotChangeDisplayName(implicit c: TestContext): Unit = {
    okTest{
      val createTableJson = Json.obj(
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table"),
        "description" -> Json.obj("de-DE" -> "Eine Tabellenbeschreibung", "en-GB" -> "Some table description")
      )
      val updateTableJson = Json.obj(
        "description" -> Json.obj("de-DE" -> null)
      )
      val baseExpected = Json.obj(
        "status" -> "ok",
        "id" -> 1,
        "name" -> "sometable",
        "displayName" -> Json.obj("de-DE" -> "Eine Tabelle", "en-GB" -> "Some table"),
        "hidden" -> false,
        "langtags" -> Json.arr("de-DE", "en-GB")
      )
      val expectedAfterCreate = baseExpected
        .copy()
        .mergeIn(Json.obj(
          "description" -> Json.obj("de-DE" -> "Eine Tabellenbeschreibung", "en-GB" -> "Some table description")))
      val expectedAfterUpdate =
        baseExpected.copy().mergeIn(Json.obj("description" -> Json.obj("en-GB" -> "Some table description")))

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
