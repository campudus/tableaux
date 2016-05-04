package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class DeleteRowTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  val expectedOkJson = Json.obj("status" -> "ok")

  @Test
  def deleteRow(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedOkJson, test)
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class ClearCellTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  val expectedOkJson = Json.obj("status" -> "ok")

  @Test
  def clearMultiLanguageCell(implicit c: TestContext): Unit = okTest {
    for {
      (table, _, _) <- createFullTableWithMultilanguageColumns("Table")

      cell <- sendRequest("DELETE", "/tables/1/columns/1/rows/1")
      cellAfterClear <- sendRequest("GET", "/tables/1/columns/1/rows/1")

      otherCellAfterClear <- sendRequest("GET", "/tables/1/columns/2/rows/1")

    } yield {
      assertEquals(Json.emptyObj(), cell.getJsonObject("value"))
      assertEquals(cell, cellAfterClear)

      assertFalse(otherCellAfterClear.isEmpty)
    }
  }
}