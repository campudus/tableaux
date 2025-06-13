package com.campudus.tableaux.api.structure

import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.testtools.RequestCreation
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class UnionTableTest extends TableauxTestBase {

  def createTableJson(name: String) = Json.obj("name" -> name)

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson

  def createNumberColumnJson(name: String, decimalDigits: Option[Int] = None) =
    RequestCreation.Columns().add(RequestCreation.NumericCol(name, decimalDigits)).getJson

  private def createUnionTable(): Future[TableId] = {
    val createColumnName = createTextColumnJson("name")
    val createColumnColor = createTextColumnJson("color")
    val createColumnGloss = createNumberColumnJson("glossLevel")

    for {
      tableId1 <- sendRequest("POST", "/tables", createTableJson("table1")).map(_.getLong("id"))
      tableId2 <- sendRequest("POST", "/tables", createTableJson("table2")).map(_.getLong("id"))
      tableId3 <- sendRequest("POST", "/tables", createTableJson("table3")).map(_.getLong("id"))

      // create columns in tables in different order
      _ <- sendRequest("POST", s"/tables/1/columns", createColumnName)
      _ <- sendRequest("POST", s"/tables/1/columns", createColumnColor)
      _ <- sendRequest("POST", s"/tables/1/columns", createColumnGloss)

      _ <- sendRequest("POST", s"/tables/2/columns", createColumnName)
      _ <- sendRequest("POST", s"/tables/2/columns", createColumnGloss)
      _ <- sendRequest("POST", s"/tables/2/columns", createColumnColor)

      _ <- sendRequest("POST", s"/tables/3/columns", createColumnGloss)
      _ <- sendRequest("POST", s"/tables/3/columns", createColumnColor)
      _ <- sendRequest("POST", s"/tables/3/columns", createColumnName)

      payload = Json.obj(
        "name" -> "union",
        "type" -> "union",
        "displayName" -> Json.obj("de" -> "Union Table"),
        "originTables" -> Json.arr(tableId1, tableId3, tableId2)
      )
      resultUnionTable <- sendRequest("POST", "/tables", payload)

    } yield resultUnionTable.getLong("id")
  }

  @Test
  def createUnionTable_withOriginTables_ok(implicit c: TestContext): Unit = okTest {
    for {
      tableId <- createUnionTable()
      unionTable <- sendRequest("GET", s"/completetable/$tableId")
    } yield {
      val expectedUnionTable = Json.obj(
        "name" -> "union",
        "type" -> "union",
        "displayName" -> Json.obj("de" -> "Union Table"),
        "originTables" -> Json.arr(1, 3, 2),
        "columns" -> Json.arr(
          Json.obj(
            "name" -> "originTable",
            "displayName" -> Json.obj("de" -> "Ursprungstabelle", "en" -> "Origin Table"),
            "description" -> Json.obj(
              "de" -> "Der Tabellenname, aus der die Daten stammen",
              "en" -> "The name of the table from which the data is taken"
            ),
            "kind" -> "text" // internally "origintable"
          )
        ),
        "rows" -> Json.emptyArr()
      )

      assertJSONEquals(expectedUnionTable, unionTable)
    }
  }

  @Test
  def createUnionTable_withoutOriginTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("unprocessable.entity") {
      for {
        _ <- sendRequest(
          "POST",
          "/tables",
          Json.obj("name" -> "union", "type" -> "union", "displayName" -> Json.obj("de" -> "Union Table"))
        )
      } yield ()
    }

  @Test
  def createUnionTable_withInvalidOriginTables_shouldFail(implicit c: TestContext): Unit =
    exceptionTest("error.database.unknown") {
      for {
        _ <- sendRequest(
          "POST",
          "/tables",
          Json.obj(
            "name" -> "union",
            "type" -> "union",
            "displayName" -> Json.obj("de" -> "Union Table"),
            "originTables" -> Json.arr(999)
          )
        )
      } yield ()
    }

  // @Test
  // def addColumnToUnionTable(implicit c: TestContext): Unit = okTest {
  //   for {
  //     tableId <- createUnionTable()

  //     column <- sendRequest(
  //       "POST",
  //       s"/tables/$tableId/columns",
  //       Json.obj("columns" -> Json.arr(Json.obj(
  //         "name" -> "test",
  //         "kind" -> "unionsimple"
  //       ))) // brauchen wir einen eigenen Typ?
  //     )
  //   } yield {
  //     val expectedColumn = Json.obj(
  //       "name" -> "test",
  //       "kind" -> "unionsimple",
  //       "displayName" -> Json.obj("de" -> "Test"),
  //       "description" -> Json.obj("de" -> "")
  //     )

  //     println(s"column: $column")

  //     assertEquals(expectedColumn, column.getJsonArray("columns").getJsonObject(0))
  //   }
  // }

  // @Test
  // def changeColumnOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.column") {
  //   for {
  //     tableId <- createSettingsTable()

  //     _ <- sendRequest("POST", s"/tables/$tableId/columns/1", Json.obj("name" -> "test"))
  //   } yield ()
  // }

  // @Test
  // def deleteColumnOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.column") {
  //   for {
  //     tableId <- createSettingsTable()

  //     _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1")
  //   } yield ()
  // }

  // @Test
  // def updateKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
  //   for {
  //     tableId <- createSettingsTable()

  //     _ <- sendRequest("POST", s"/tables/$tableId/rows")

  //     _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", Json.obj("value" -> "another value"))
  //   } yield ()
  // }

  // @Test
  // def replaceKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
  //   for {
  //     tableId <- createSettingsTable()

  //     _ <- sendRequest("POST", s"/tables/$tableId/rows")

  //     _ <- sendRequest("PUT", s"/tables/$tableId/columns/1/rows/1", Json.obj("value" -> "another value"))
  //   } yield ()
  // }

  // @Test
  // def clearKeyCellOfSettingsTable(implicit c: TestContext): Unit = exceptionTest("error.request.forbidden.cell") {
  //   for {
  //     tableId <- createSettingsTable()

  //     _ <- sendRequest("POST", s"/tables/$tableId/rows")

  //     _ <- sendRequest("DELETE", s"/tables/$tableId/columns/1/rows/1")
  //   } yield ()
  // }

  // @Test
  // def changeDisplayKeyCellOfSettingsTable(implicit c: TestContext): Unit = {
  //   exceptionTest("error.request.forbidden.cell") {
  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows")

  //       _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/1", Json.obj("value" -> "another value"))
  //     } yield ()
  //   }
  // }

  // @Test
  // def deleteRowOfSettingsTable(implicit c: TestContext): Unit =
  //   okTest {
  //     val expectedOkJson = Json.obj("status" -> "ok")

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows")

  //       test <- sendRequest("DELETE", s"/tables/$tableId/rows/1")
  //     } yield assertEquals(expectedOkJson, test)
  //   }

  // @Test
  // def insertKeyIntoSettingsTable(implicit c: TestContext): Unit =
  //   okTest {
  //     def settingsRow = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 1)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr("key")))
  //     )

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow)
  //       test <- sendRequest("GET", s"/tables/$tableId/rows")
  //     } yield {
  //       assertEquals(test.getJsonArray("rows").size(), 1)
  //     }
  //   }

  // @Test
  // def insertDuplicateKeyIntoSettingsTable(implicit c: TestContext): Unit =
  //   exceptionTest("error.request.unique.cell") {
  //     def settingsRow = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 1)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr("already_existing_key")))
  //     )

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow)
  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow)
  //     } yield ()
  //   }

  // @Test
  // def insertDuplicateKeyIntoSettingsTableWithKeyColumnIsNotFirstColumn(implicit c: TestContext): Unit =
  //   exceptionTest("error.request.unique.cell") {
  //     def settingsRow1 = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 3), Json.obj("id" -> 1)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "value"), "already_existing_key")))
  //     )

  //     def settingsRow2 = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 3), Json.obj("id" -> 1)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "another_value"), "already_existing_key")))
  //     )

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow1)
  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRow2)
  //     } yield ()
  //   }

  // @Test
  // def insertEmptyKeyIntoSettingsTable(implicit c: TestContext): Unit =
  //   exceptionTest("error.request.invalid") {
  //     def settingsRowWithEmptyKey = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 1)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr("")))
  //     )

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRowWithEmptyKey)
  //     } yield ()
  //   }

  // @Test
  // def insertNullKeyIntoSettingsTable(implicit c: TestContext): Unit =
  //   exceptionTest("error.request.invalid") {
  //     def settingsRowWithNullKey = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 1)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr(null)))
  //     )

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRowWithNullKey)
  //     } yield ()
  //   }

  // @Test
  // def insertWithoutKeyIntoSettingsTable(implicit c: TestContext): Unit =
  //   exceptionTest("error.request.invalid") {
  //     def settingsRowWithoutKeyColumn = Json.obj(
  //       "columns" -> Json.arr(Json.obj("id" -> 3)),
  //       "rows" -> Json.arr(Json.obj("values" -> Json.arr(Json.obj("de-DE" -> "any_value"))))
  //     )

  //     for {
  //       tableId <- createSettingsTable()

  //       _ <- sendRequest("POST", s"/tables/$tableId/rows", settingsRowWithoutKeyColumn)
  //     } yield ()
  //   }
}
