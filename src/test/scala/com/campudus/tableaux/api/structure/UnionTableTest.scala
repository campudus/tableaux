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

  def createMultilangTextColumnJson(name: String) =
    RequestCreation.Columns().add(RequestCreation.Multilanguage(RequestCreation.TextCol(name))).getJson

  def createNumberColumnJson(name: String, decimalDigits: Option[Int] = None) =
    RequestCreation.Columns().add(RequestCreation.NumericCol(name, decimalDigits)).getJson

  // format: off
  /**
   * Creates three almost identical tables but with different column order.
   * 
   * - column "name" is of type text, single language
   * - column "color" is of type text, multi language
   * - column "glossLevel" is of type number, single language
   *
   * table1 ┌──┬────┬─────┬──────────┐
   *        │id│name│color│glossLevel│
   *        ├──┼────┼─────┼──────────┤
   *        │0 │1   │2    │3         │
   *        └──┴────┴─────┴──────────┘
   * table2 ┌──┬────┬─────┬──────────┐
   *        │id│name│color│glossLevel│
   *        ├──┼────┼─────┼──────────┤
   *        │0 │1   │3    │2         │
   *        └──┴────┴─────┴──────────┘
   * table3 ┌──┬────┬─────┬──────────┐
   *        │id│name│color│glossLevel│
   *        ├──┼────┼─────┼──────────┤
   *        │0 │3   │2    │1         │
   *        └──┴────┴─────┴──────────┘
   */
  // format: on
  private def createUnionTable(): Future[TableId] = {
    val createColumnName = createTextColumnJson("name")
    val createColumnColor = createMultilangTextColumnJson("color")
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

  @Test
  def addSimpleColumnsToUnionTable(implicit c: TestContext): Unit = okTest {
    val unionTableCol1 = Json.obj(
      "name" -> "name",
      "kind" -> "text",
      "ordering" -> 1,
      "displayName" -> Json.obj("de" -> "Marketing Name", "en" -> "Marketing Name"),
      "description" -> Json.obj("de" -> "Marketingname der Farbe", "en" -> "marketing name of the color"),
      "originColumns" -> Json.obj(
        "1" -> Json.obj("id" -> 1),
        "2" -> Json.obj("id" -> 1),
        "3" -> Json.obj("id" -> 3)
      )
    )
    val unionTableCol2 = Json.obj(
      "name" -> "color",
      "kind" -> "text",
      "ordering" -> 2,
      "displayName" -> Json.obj("de" -> "Name der Farbe", "en" -> "Color Name"),
      "description" -> Json.obj("de" -> "Name der Farbe", "en" -> "Name of the color"),
      "originColumns" -> Json.obj(
        "1" -> Json.obj("id" -> 2),
        "2" -> Json.obj("id" -> 3),
        "3" -> Json.obj("id" -> 2)
      )
    )
    val unionTableCol3 = Json.obj(
      "name" -> "glossLevel",
      "kind" -> "numeric",
      "ordering" -> 3,
      "displayName" -> Json.obj("de" -> "Glanzgrad", "en" -> "Gloss Level"),
      "description" -> Json.obj("de" -> "Der Glanzgrad der Farbe", "en" -> "The gloss level of the color"),
      "originColumns" -> Json.obj(
        "1" -> Json.obj("id" -> 3),
        "2" -> Json.obj("id" -> 2),
        "3" -> Json.obj("id" -> 1)
      )
    )

    for {
      tableId <- createUnionTable()

      columns <- sendRequest(
        "POST",
        s"/tables/$tableId/columns",
        Json.obj("columns" -> Json.arr(unionTableCol1, unionTableCol2, unionTableCol3))
      )
    } yield {
      // val expectedColumn = Json.obj(
      //   "name" -> "test",
      //   "kind" -> "unionsimple",
      //   "displayName" -> Json.obj("de" -> "Test"),
      //   "description" -> Json.obj("de" -> "")
      // )

      // println(s"column: $column")

      // assertEquals(expectedColumn, column.getJsonArray("columns").getJsonObject(0))
    }
  }
}
