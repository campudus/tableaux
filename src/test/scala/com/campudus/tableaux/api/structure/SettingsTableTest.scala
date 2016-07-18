package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class SettingsTableTest extends TableauxTestBase {

  private def createSettingsTable(): Future[TableId] = {
    for {
      result <- sendRequest("POST", "/tables", Json.obj("name" -> "settings", "type" -> "settings", "displayName" -> Json.obj("de" -> "Settings Table")))
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

      assertContainsDeep(expectedSettingsTable, settingsTable)
    }
  }

  @Test
  def createTableWithInvalidTableType(implicit c: TestContext): Unit = exceptionTest("error.arguments") {
    for {
      result <- sendRequest("POST", "/tables", Json.obj("name" -> "settings", "type" -> "sgnittes", "displayName" -> Json.obj("de" -> "Settings Table")))
    } yield ()
  }

  @Test
  def addColumnToSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.column") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/columns", Json.obj("columns" -> Json.arr(Json.obj("name" -> "test", "kind" -> "text"))))
    } yield ()
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
  def changeDisplayKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/1", Json.obj("value" -> "another value"))
    } yield ()
  }

  @Test
  def deleteRowOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.row") {
    for {
      tableId <- createSettingsTable()

      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      _ <- sendRequest("DELETE", s"/tables/$tableId/rows/1")
    } yield ()
  }
}
