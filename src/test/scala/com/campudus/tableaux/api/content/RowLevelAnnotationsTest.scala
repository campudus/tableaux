package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation.{Identifier, TextCol}
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._

import org.junit.{Ignore, Test}
import org.junit.Assert._
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class RowLevelAnnotationsTest extends TableauxTestBase {

  @Test
  def createRowAndSetFinalFlag(implicit c: TestContext): Unit = {
    okTest {
      for {
        tableId <- createEmptyDefaultTable()

        // empty row
        result <- sendRequest("POST", s"/tables/$tableId/rows")
        rowId = result.getLong("id")

        _ <- sendRequest("PATCH", s"/tables/$tableId/rows/$rowId/annotations", Json.obj("final" -> true))

        rowJson1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")

        _ <- sendRequest("PATCH", s"/tables/$tableId/rows/$rowId/annotations", Json.obj("final" -> false))

        rowJson2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
      } yield {
        val expectedRowJson1 = Json.obj(
          "status" -> "ok",
          "id" -> rowId,
          "final" -> true,
          "values" -> Json.arr(null, null)
        )

        assertJSONEquals(expectedRowJson1, rowJson1)

        val expectedRowJson2 = Json.obj(
          "status" -> "ok",
          "id" -> rowId,
          "values" -> Json.arr(null, null)
        )

        assertJSONEquals(expectedRowJson2, rowJson2)
      }
    }
  }
}
