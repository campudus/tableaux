package com.campudus.tableaux

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class MultiLanguageTest extends TableauxTestBase {

  @Test
  def testCreateAndDeleteMultilanguageColumn(implicit c: TestContext): Unit = okTest {
    def exceptedJson(tableId: TableId, columnId: ColumnId) = Json.obj(
      "status" -> "ok",
      "id" -> columnId,
      "name" -> "Test Column 1",
      "kind" -> "text",
      "ordering" -> columnId,
      "multilanguage" -> true
    )

    for {
      (tableId, columnIds) <- createTableWithMultilanguageColumns("Multilanguage Table")
      columnId = columnIds.head

      column <- sendRequest("GET", s"/tables/$tableId/columns/$columnId")

      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId")

      columnsAfterDelete <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      assertEquals(exceptedJson(tableId, columnId), column)
      assertEquals(6, columnsAfterDelete.getArray("columns").size())
    }
  }

  @Test
  def testFillMultilanguageCell(implicit c: TestContext): Unit = okTest {
    val cellValue = Json.obj(
      "value" -> Json.obj(
        "de_DE" -> "Hallo, Welt!",
        "en_US" -> "Hello, World!"
      )
    )

    val exceptedJson = Json.fromObjectString(
      """
        |{
        |  "status" : "ok",
        |  "value" : {
        |    "de_DE" : "Hallo, Welt!",
        |    "en_US" : "Hello, World!"
        |  }
        |}
      """.stripMargin)

    for {
      (tableId, columnIds) <- createTableWithMultilanguageColumns("Multilanguage Table")
      columnId = columnIds.head

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getLong("id"))

      _ <- sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$rowId", cellValue)

      cell <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")
    } yield {
      assertEquals(exceptedJson, cell)
    }
  }

  @Test
  def testEmptyMultilanguageCell(implicit c: TestContext): Unit = okTest {

    val exceptedJson = Json.fromObjectString(
      """
        |{
        |  "status" : "ok",
        |  "value" : {}
        |}
      """.stripMargin)

    for {
      (tableId, columnIds) <- createTableWithMultilanguageColumns("Multilanguage Table")
      columnId = columnIds.head

      row <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = row.getLong("id")

      cell <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")
    } yield {
      assertEquals(exceptedJson, cell)
    }
  }

  @Test
  def testFillMultilanguageRow(implicit c: TestContext): Unit = okTest {
    val valuesRow = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1)),
      "rows" -> Json.arr(
        Json.obj("values" ->
          Json.arr(
            Json.obj(
              "de_DE" -> "Hallo, Welt!",
              "en_US" -> "Hello, World!"
            )
          )
        )
      )
    )

    def exceptedJson(rowId: RowId) = Json.fromObjectString(
      s"""
         |{
         | "status" : "ok",
         | "id" : $rowId,
         | "values" : [
         |  {"de_DE" : "Hallo, Welt!", "en_US" : "Hello, World!"},
         |  {},
         |  {},
         |  {},
         |  {},
         |  {},
         |  {}
         | ]
         |}
      """.stripMargin)

    for {
      (tableId, columnId) <- createTableWithMultilanguageColumns("Multilanguage Table")

      rowId1 <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow) map (_.getArray("rows").get[JsonObject](0).getLong("id"))
      rowId2 <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow) map (_.getArray("rows").get[JsonObject](0).getLong("id"))
      rowId3 <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow) map (_.getArray("rows").get[JsonObject](0).getLong("id"))

      row1 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId1")
      row2 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId2")
      row3 <- sendRequest("GET", s"/tables/$tableId/rows/$rowId3")
    } yield {
      assertEquals(exceptedJson(rowId1), row1)
      assertEquals(exceptedJson(rowId2), row2)
      assertEquals(exceptedJson(rowId3), row3)
    }
  }

  @Test
  def testSingleTranslation(implicit c: TestContext): Unit = okTest {
    val valuesRow = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1)),
      "rows" -> Json.arr(
        Json.obj("values" ->
          Json.arr(
            Json.obj(
              "de_DE" -> "Hallo, Welt!",
              "en_US" -> "Hello, World!"
            )
          )
        )
      )
    )

    val cellTextValue = Json.obj(
      "value" -> Json.obj(
        "en_US" -> "Hello, Cell!"
      )
    )

    val cellBooleanValue = Json.obj(
      "value" -> Json.obj(
        "de_DE" -> true
      )
    )

    val cellNumericValue = Json.obj(
      "value" -> Json.obj(
        "de_DE" -> 3.1415926
      )
    )

    val cellDateValue = Json.obj(
      "value" -> Json.obj(
        "de_DE" -> "2015-01-01"
      )
    )

    val cellDateTimeValue = Json.obj(
      "value" -> Json.obj(
        "de_DE" -> "2015-01-01T14:37:47.110+01"
      )
    )

    val exceptedJson = Json.fromObjectString(
      """
        |{
        |  "status" : "ok",
        |  "id" : 1,
        |  "values" : [
        |   { "de_DE" : "Hallo, Welt!", "en_US" : "Hello, Cell!" },
        |   { "de_DE" : true },
        |   { "de_DE" : 3.1415926 },
        |   { "en_US" : "Hello, Cell!" },
        |   { "en_US" : "Hello, Cell!" },
        |   { "de_DE" : "2015-01-01" },
        |   { "de_DE" : "2015-01-01T13:37:47.110Z" }
        |  ]
        |}
      """.stripMargin)

    for {
      (tableId, _) <- createTableWithMultilanguageColumns("Multilanguage Table")

      rowId <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow) map (_.getArray("rows").get[JsonObject](0).getLong("id"))

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId", cellTextValue)

      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId", cellBooleanValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/3/rows/$rowId", cellNumericValue)

      _ <- sendRequest("POST", s"/tables/$tableId/columns/4/rows/$rowId", cellTextValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/5/rows/$rowId", cellTextValue)

      _ <- sendRequest("POST", s"/tables/$tableId/columns/6/rows/$rowId", cellDateValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/7/rows/$rowId", cellDateTimeValue)

      row <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      assertEquals(exceptedJson, row)
    }
  }

}