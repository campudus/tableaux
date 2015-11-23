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
      (tableId, columnId) <- createTableWithMultilanguageColumn()

      column <- sendRequest("GET", s"/tables/$tableId/columns/$columnId")

      _ <- sendRequest("DELETE", s"/tables/$tableId/columns/$columnId")

      columnsAfterDelete <- sendRequest("GET", s"/tables/$tableId/columns")
    } yield {
      assertEquals(exceptedJson(tableId, columnId), column)
      assertEquals(4, columnsAfterDelete.getArray("columns").size())
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
      (tableId, columnId) <- createTableWithMultilanguageColumn()

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
      (tableId, columnId) <- createTableWithMultilanguageColumn()

      rowId <- sendRequest("POST", s"/tables/$tableId/rows") map (_.getLong("id"))

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
         |  {}
         | ]
         |}
      """.stripMargin)

    for {
      (tableId, columnId) <- createTableWithMultilanguageColumn()

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
        |   { "en_US" : "Hello, Cell!" }
        |  ]
        |}
      """.stripMargin)

    for {
      (tableId, _) <- createTableWithMultilanguageColumn()

      rowId <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow) map (_.getArray("rows").get[JsonObject](0).getLong("id"))

      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/$rowId", cellTextValue)

      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/$rowId", cellBooleanValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/3/rows/$rowId", cellNumericValue)

      _ <- sendRequest("POST", s"/tables/$tableId/columns/4/rows/$rowId", cellTextValue)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/5/rows/$rowId", cellTextValue)

      row <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      assertEquals(exceptedJson, row)
    }
  }

  private def createTableWithMultilanguageColumn(): Future[(Long, Long)] = {
    val createMultilanguageColumn = Json.obj(
      "columns" ->
        Json.arr(
          Json.obj("kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> true),
          Json.obj("kind" -> "boolean", "name" -> "Test Column 2", "multilanguage" -> true),
          Json.obj("kind" -> "numeric", "name" -> "Test Column 3", "multilanguage" -> true),
          Json.obj("kind" -> "richtext", "name" -> "Test Column 4", "multilanguage" -> true),
          Json.obj("kind" -> "shorttext", "name" -> "Test Column 5", "multilanguage" -> true)
        )
    )

    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "Multi Language")) map (_.getLong("id"))
      columnId <- sendRequest("POST", s"/tables/$tableId/columns", createMultilanguageColumn) map (_.getArray("columns").get[JsonObject](0).getLong("id"))
    } yield {
      (tableId.toLong, columnId.toLong)
    }
  }
}