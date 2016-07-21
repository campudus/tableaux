package com.campudus.tableaux.api.structure

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class TableGroupTest extends TableauxTestBase {

  @Test
  def createTableGroup(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")
    } yield {
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), createdGroup)
    }
  }

  @Test
  def createTableGroupWithInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.groups") {
    val invalidCreateTableGroupJson = Json.obj(
      "displayName" -> Json.obj(),
      "description" -> null
    )

    for {
      _ <- sendRequest("POST", "/groups", invalidCreateTableGroupJson)
    } yield ()
  }

  @Test
  def changeTableGroupWithExistingLangtag(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    val changeTableGroupJson = Json.obj("displayName" -> Json.obj("de" -> "unlustig"))

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      updatedGroup <- sendRequest("POST", s"/groups/$groupId", changeTableGroupJson)
    } yield {
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), createdGroup)
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson).mergeIn(changeTableGroupJson), updatedGroup)
    }
  }

  @Test
  def changeTableGroupWithNewLangtag(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    val changeTableGroupJson = Json.obj("displayName" -> Json.obj("fr" -> "lé unlustisch"))

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      updatedGroup <- sendRequest("POST", s"/groups/$groupId", changeTableGroupJson)
    } yield {
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), createdGroup)
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson).mergeIn(changeTableGroupJson), updatedGroup)
    }
  }

  @Test
  def changeTableGroupWithInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.arguments") {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    val invalidChangeTableGroupJson = Json.obj("displayName" -> Json.obj())

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      _ <- sendRequest("POST", s"/groups/$groupId", invalidChangeTableGroupJson)
    } yield ()
  }

  @Test
  def assignTableGroupToExistingTable(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    for {
      tableId <- createEmptyDefaultTable()

      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      updatedTable <- sendRequest("POST", s"/tables/$tableId", Json.obj("group" -> groupId))

      retrieveTable <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), updatedTable.getJsonObject("group"))
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), retrieveTable.getJsonObject("group"))
    }
  }

  @Test
  def assignNoTableGroupToExistingTable(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    for {
      tableId <- createEmptyDefaultTable()

      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      _ <- sendRequest("POST", s"/tables/$tableId", Json.obj("group" -> groupId))
      _ <- sendRequest("POST", s"/tables/$tableId", Json.obj("group" -> null))

      retrieveTable <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertFalse("Should contain a group object", retrieveTable.containsKey("group"))
    }
  }

  @Test
  def assignTableGroupToNewTable(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      createdTable <- sendRequest("POST", s"/tables", Json.obj("name" -> "test", "group" -> groupId))
      tableId = createdTable.getInteger("id")

      retrieveTable <- sendRequest("GET", s"/tables/$tableId")
    } yield {
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), createdTable.getJsonObject("group"))
      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), retrieveTable.getJsonObject("group"))
    }
  }

  @Test
  def retrieveAllTablesWithTableGroups(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      createdTable <- sendRequest("POST", s"/tables", Json.obj("name" -> "test", "group" -> groupId))
      tableId = createdTable.getInteger("id")

      retrieveTables <- sendRequest("GET", s"/tables")
    } yield {
      import com.campudus.tableaux.helper.JsonUtils._
      val tables = asCastedList[JsonObject](retrieveTables.getJsonArray("tables")).get

      assertEquals(1, tables.size)

      assertContainsDeep(Json.obj("id" -> groupId).mergeIn(createTableGroupJson), tables.head.getJsonObject("group"))
    }
  }

  @Test
  def retrieveAllTablesWithTableGroupsAfterDeletingTableGroup(implicit c: TestContext): Unit = okTest {
    val createTableGroupJson = Json.obj(
      "displayName" -> Json.obj("de" -> "lustig", "en" -> "funny"),
      "description" -> Json.obj("de" -> "Kommentar", "en" -> "comment")
    )

    for {
      createdGroup <- sendRequest("POST", "/groups", createTableGroupJson)
      groupId = createdGroup.getInteger("id")

      createdTable <- sendRequest("POST", s"/tables", Json.obj("name" -> "test", "group" -> groupId))
      tableId = createdTable.getInteger("id")

      _ <- sendRequest("DELETE", s"/groups/$groupId")

      retrieveTables <- sendRequest("GET", s"/tables")
    } yield {
      import com.campudus.tableaux.helper.JsonUtils._
      val tables = asCastedList[JsonObject](retrieveTables.getJsonArray("tables")).get

      assertEquals(1, tables.size)

      assertFalse(tables.head.containsKey("group"))
    }
  }
}