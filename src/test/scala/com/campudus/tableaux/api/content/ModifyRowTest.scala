package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.domain.Cardinality
import com.campudus.tableaux.database.domain.CellAnnotationType
import com.campudus.tableaux.database.domain.Constraint
import com.campudus.tableaux.database.model.TableauxModel
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.JsonAssertable
import com.campudus.tableaux.testtools.RequestCreation
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class ModifyRowTest extends TableauxTestBase {

  @Test
  def updateRow(implicit c: TestContext): Unit = {
    val columnsPayload = Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2))
    val valuesPayload = Json.arr("some-string", 42)
    val payload = Json.obj("columns" -> columnsPayload, "values" -> valuesPayload)

    okTest {
      for {
        tableId <- createDefaultTable()
        updated <- sendRequest("PATCH", s"/tables/$tableId/rows/1", payload)
        updatedValues = updated.getJsonArray("values")
      } yield assertEquals(updatedValues, valuesPayload)
    }
  }

  @Test
  def replaceRow(implicit c: TestContext): Unit = {
    val addColumnPayload = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "shorttext",
      "languageType" -> "language",
      "name" -> "thirdcolumn"
    )))
    val columnsPayload = Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2), Json.obj("id" -> 3))
    val valuesPayload = Json.arr("some-string", 42, Json.obj("de" -> "TESTWERT"))
    val payload = Json.obj("columns" -> columnsPayload, "values" -> valuesPayload)

    okTest {
      for {
        // Setup
        tableId <- createDefaultTable()
        _ <- sendRequest("POST", s"/tables/$tableId/columns", addColumnPayload)
        _ <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/3/rows/1",
          Json.obj("value" -> Json.obj("de" -> "Überprüfungswert", "en" -> "testvalue"))
        )

        // Test
        updated <- sendRequest("PUT", s"/tables/$tableId/rows/1", payload)
        updatedValues = updated.getJsonArray("values")
      } yield assertEquals(updatedValues, valuesPayload)
    }
  }
}
