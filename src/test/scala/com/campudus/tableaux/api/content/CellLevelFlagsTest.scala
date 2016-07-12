package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class CellLevelFlagsTest extends TableauxTestBase {

  @Test
  def setFlagWithoutLangtag(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createEmptyDefaultTable()

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("type" -> "info", "value" -> "this is a comment"))

      rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      val exceptedColumn1Flags = Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment"))

      assertContainsDeep(exceptedColumn1Flags, rowJson1.getJsonArray("cellFlags").getJsonArray(0))
      assertNull(rowJson1.getJsonArray("cellFlags").getJsonArray(1))
    }
  }

  @Test
  def setMultipleFlagsWithoutLangtag(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createEmptyDefaultTable()

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("type" -> "error"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("type" -> "info", "value" -> "this is a comment"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/flags", Json.obj("type" -> "warning"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/flags", Json.obj("type" -> "error", "value" -> "this is another comment"))

      rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      uuid = rowJson1.getJsonArray("cellFlags").getJsonArray(0).getJsonObject(0).getString("uuid")
      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/$rowId/flags/$uuid")

      rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      val exceptedColumn1Flags = Json.arr(Json.obj("type" -> "error", "value" -> null), Json.obj("type" -> "info", "value" -> "this is a comment"))
      val exceptedColumn2Flags = Json.arr(Json.obj("type" -> "warning", "value" -> null), Json.obj("type" -> "error", "value" -> "this is another comment"))

      assertContainsDeep(exceptedColumn1Flags, rowJson1.getJsonArray("cellFlags").getJsonArray(0))
      assertContainsDeep(exceptedColumn2Flags, rowJson1.getJsonArray("cellFlags").getJsonArray(1))

      val exceptedColumn1FlagsAfterDelete = Json.arr(Json.obj("type" -> "info", "value" -> "this is a comment"))

      assertContainsDeep(exceptedColumn1FlagsAfterDelete, rowJson2.getJsonArray("cellFlags").getJsonArray(0))
    }
  }

  @Test
  def setMultipleFlagsWithLangtag(implicit c: TestContext): Unit = okTest {
    for {
      (tableId, _) <- createTableWithMultilanguageColumns("Test")

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("langtag" -> "de", "type" -> "error"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("langtag" -> "gb", "type" -> "info", "value" -> "this is a comment"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/flags", Json.obj("langtag" -> "gb", "type" -> "warning"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId/flags", Json.obj("langtag" -> "de", "type" -> "error", "value" -> "this is another comment"))

      rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      val exceptedColumn1Flags = Json.arr(Json.obj("langtag" -> "de", "type" -> "error", "value" -> null), Json.obj("langtag" -> "gb", "type" -> "info", "value" -> "this is a comment"))
      val exceptedColumn2Flags = Json.arr(Json.obj("langtag" -> "gb", "type" -> "warning", "value" -> null), Json.obj("langtag" -> "de", "type" -> "error", "value" -> "this is another comment"))

      assertContainsDeep(exceptedColumn1Flags, rowJson1.getJsonArray("cellFlags").getJsonArray(0))
      assertContainsDeep(exceptedColumn2Flags, rowJson1.getJsonArray("cellFlags").getJsonArray(1))
    }
  }

  @Test
  def setFlagWithLangtagOnLanguageNeutralCell(implicit c: TestContext): Unit = exceptionTest("unprocessable.entity") {
    for {
      tableId <- createDefaultTable("Test")

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("langtag" -> "de", "type" -> "error"))

    } yield ()
  }

  @Test
  def setInvalidFlag(implicit c: TestContext): Unit = exceptionTest("error.arguments") {
    for {
      tableId <- createDefaultTable("Test")

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId/flags", Json.obj("type" -> "invalid"))

    } yield ()
  }
}