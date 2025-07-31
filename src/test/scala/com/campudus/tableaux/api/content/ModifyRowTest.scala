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
  def test1(implicit c: TestContext): Unit = {
    val columnsPayload = Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2))
    val valuesPayload = Json.arr("some-string", 42)
    val payload = Json.obj("columns" -> columnsPayload, "values" -> valuesPayload)

    println(payload)

    okTest {
      for {
        tableId <- createDefaultTable()
        _ <- Future.successful(println("TableID:", tableId))
        updated <- sendRequest("PATCH", s"/tables/$tableId/rows/1", payload)

      } yield assertEquals(1, 1)
    }
  }
}
