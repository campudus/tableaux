package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation.{Identifier, TextCol}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._

@RunWith(classOf[VertxUnitRunner])
class RowLevelAnnotationsTest extends TableauxTestBase {

  @Test
  def createRowAndSetFinalFlag(implicit c: TestContext): Unit = {
    okTest{
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

        assertEquals(expectedRowJson1, rowJson1)

        val expectedRowJson2 = Json.obj(
          "status" -> "ok",
          "id" -> rowId,
          "values" -> Json.arr(null, null)
        )

        assertEquals(expectedRowJson2, rowJson2)
      }
    }
  }

  @Test
  def createRowsAndSetFinalFlagForTable(implicit c: TestContext): Unit = {
    okTest{
      for {
        (tableId, _, _) <- createSimpleTableWithValues(
          "table",
          Seq(Identifier(TextCol("text"))),
          Seq(
            Seq("row 1"),
            Seq("row 2"),
            Seq("row 3")
          )
        )

        _ <- sendRequest("PATCH", s"/tables/$tableId/rows/annotations", Json.obj("final" -> true))

        rowsAllFinal <- sendRequest("GET", s"/tables/$tableId/rows")

        _ <- sendRequest("PATCH", s"/tables/$tableId/rows/annotations", Json.obj("final" -> false))

        rowsAllNotFinal <- sendRequest("GET", s"/tables/$tableId/rows")
      } yield {
        val rowsAreFinal = rowsAllFinal
          .getJsonArray("rows", Json.emptyArr())
          .asScala
          .toList
          .map(_.asInstanceOf[JsonObject])
          .forall(_.getBoolean("final"))

        assertTrue(rowsAreFinal)

        val rowsAreNotFinal = rowsAllNotFinal
          .getJsonArray("rows", Json.emptyArr())
          .asScala
          .toList
          .map(_.asInstanceOf[JsonObject])
          .forall(!_.containsField("final"))

        assertTrue(rowsAreNotFinal)
      }
    }
  }
}