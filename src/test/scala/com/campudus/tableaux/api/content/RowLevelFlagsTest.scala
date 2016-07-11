package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class RowLevelFlagsTest extends TableauxTestBase {

  @Test
  def createRowAndSetFinalFlag(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createEmptyDefaultTable()

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/flags", Json.obj("final" -> true))

      rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      _ <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/flags", Json.obj("final" -> false))

      rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      val expectedRowJson1 = Json.obj(
        "status" -> "ok",
        "id" -> rowId,
        "final" -> true,
        "needsTranslation" -> Json.arr(),
        "values" -> Json.arr(null, null)
      )

      assertEquals(expectedRowJson1, rowJson1)

      val expectedRowJson2 = Json.obj(
        "status" -> "ok",
        "id" -> rowId,
        "values" -> Json.arr(null, null)
      )

      assertEquals(expectedRowJson2, rowJson2)
    }
  }

  @Test
  def createRowAndSetNeedsTranslationFlags(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createEmptyDefaultTable()

      // empty row
      result <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = result.getLong("id")

      _ <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/flags", Json.obj("needsTranslation" -> Json.arr("de", "en", "gb")))
      rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      _ <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/flags", Json.obj("needsTranslation" -> Json.arr("de", "gb")))
      rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      _ <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/flags", Json.obj("needsTranslation" -> Json.arr()))
      rowJson3 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

      _ <- sendRequest("POST", s"/tables/$tableId/rows/$rowId/flags", Json.obj("needsTranslation" -> null))
      rowJson4 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      val expectedRowJson1 = Json.obj(
        "status" -> "ok",
        "id" -> rowId,
        "final" -> false,
        "needsTranslation" -> Json.arr("de", "en", "gb"),
        "values" -> Json.arr(null, null)
      )

      assertEquals(expectedRowJson1, rowJson1)

      val expectedRowJson2 = Json.obj(
        "status" -> "ok",
        "id" -> rowId,
        "final" -> false,
        "needsTranslation" -> Json.arr("de", "gb"),
        "values" -> Json.arr(null, null)
      )

      assertEquals(expectedRowJson2, rowJson2)

      val expectedRowJson3 = Json.obj(
        "status" -> "ok",
        "id" -> rowId,
        "values" -> Json.arr(null, null)
      )

      assertEquals(expectedRowJson3, rowJson3)
      assertEquals(expectedRowJson3, rowJson4)
    }
  }
}