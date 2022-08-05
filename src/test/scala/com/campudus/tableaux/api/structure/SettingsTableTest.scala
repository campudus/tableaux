package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class SettingsTableTest extends TableauxTestBase {

  private def createSettingsTable(): Future[TableId] = {
    for {
      result <- sendRequest(
        "POST",
        "/tables",
        Json.obj("name" -> "settings", "type" -> "settings", "displayName" -> Json.obj("de" -> "Settings Table"))
      )
    } yield result.getLong("id")
  }

  @Test
  def createSettingsTable(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createSettingsTable()
      settingsTable <- sendRequest("GET", s"/completetable/$tableId")
    } yield {
      val expectedSettingsTable = Json.obj(
        "name" -> "settings",
        "type" -> "settings",
        "displayName" -> Json.obj("de" -> "Settings Table"),
        "columns" -> Json.arr(
          Json.obj("name" -> "key", "kind" -> "shorttext"),
          Json.obj("name" -> "displayKey", "kind" -> "shorttext"),
          Json.obj("name" -> "value", "kind" -> "text"),
          Json.obj("name" -> "attachment", "kind" -> "attachment")
        ),
        "rows" -> Json.emptyArr()
      )

      assertJSONEquals(expectedSettingsTable, settingsTable)
    }
  }

  @Test
  def createTableWithInvalidTableType(implicit c: TestContext): Unit = {
    exceptionTest("error.arguments") {
      for {
        result <- sendRequest(
          "POST",
          "/tables",
          Json.obj("name" -> "settings", "type" -> "sgnittes", "displayName" -> Json.obj("de" -> "Settings Table"))
        )
      } yield ()
    }
  }

  @Test
  def addColumnToSettingsTable(implicit c: TestContext): Unit = {
    exceptionTest("error.request.forbidden.column") {
      for {
        tableId <- createSettingsTable()

        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns",
          Json.obj("columns" -> Json.arr(Json.obj("name" -> "test", "kind" -> "text")))
        )
      } yield ()
    }
  }

  @Test
  def changeColumnOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1", Json.obj("name" -> "test"))
    } yield ()
  }

  @Test
  def deleteColumnOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1")
    } yield ()
  }

  @Test
  def updateKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", Json.obj("value" -> "another value"))
    } yield ()
  }

  @Test
  def replaceKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      _ <- sendRequest("PUT", s"/tables/$tableId/columns/1/rows/1", Json.obj("value" -> "another value"))
    } yield ()
  }

  @Test
  def clearKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1")
    } yield ()
  }

  @Test
  def changeDisplayKeyCellOfSettingsTable(implicit c: TestContext): Unit = {
    exceptionTest("error.request.forbidden.cell") {
      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows")

        _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/1", Json.obj("value" -> "another value"))
      } yield ()
    }
  }

  @Test
  def deleteRowOfSettingsTable(implicit c: TestContext): Unit =
    okTest {
      val expectedOkJson = Json.obj("status" -> "ok")

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows")

        test <- sendRequest("DELETE", s"/tables/$tableId/rows/1")
      } yield assertEquals(expectedOkJson, test)
    }

  @Test
  def insertKeyIntoSettingsTable(implicit c: TestContext): Unit =
    okTest {
      def settingsRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr("key")))
      )

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow)
        test <- sendRequest("GET", s"/tables/$tableId/rows")
      } yield {
        assertEquals(test.getJsonArray("rows").size(), 1)
      }
    }

  @Test
  def insertDuplicateKeyIntoSettingsTable(implicit c: TestContext): Unit =
    exceptionTest("error.request.unique.cell") {
      def settingsRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr("already_existing_key")))
      )

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow)
        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow)
      } yield ()
    }

  @Test
  def insertDuplicateKeyIntoSettingsTableWithKeyColumnIsNotFirstColumn(implicit c: TestContext): Unit =
    exceptionTest("error.request.unique.cell") {
      def settingsRow1 = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 3), Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "value"), "already_existing_key")))
      )

      def settingsRow2 = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 3), Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "another_value"), "already_existing_key")))
      )

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow1)
        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow2)
      } yield ()
    }

  @Test
  def insertEmptyKeyIntoSettingsTable(implicit c: TestContext): Unit =
    exceptionTest("error.request.invalid") {
      def settingsRowWithEmptyKey = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr("")))
      )

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRowWithEmptyKey)
      } yield ()
    }

  @Test
  def insertNullKeyIntoSettingsTable(implicit c: TestContext): Unit =
    exceptionTest("error.request.invalid") {
      def settingsRowWithNullKey = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(null)))
      )

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRowWithNullKey)
      } yield ()
    }

  @Test
  def insertWithoutKeyIntoSettingsTable(implicit c: TestContext): Unit =
    exceptionTest("error.request.invalid") {
      def settingsRowWithoutKeyColumn = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 3)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "any_value"))))
      )

      for {
        tableId <- createSettingsTable()

        _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRowWithoutKeyColumn)
      } yield ()
    }
}
