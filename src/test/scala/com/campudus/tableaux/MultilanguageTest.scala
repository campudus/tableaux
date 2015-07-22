package com.campudus.tableaux

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import org.junit.{Ignore, Test}
import org.vertx.java.core.json.JsonObject
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class MultilanguageTest extends TableauxTestBase {

  @Test
  def testCreateAndDeleteMultilanguageColumn(): Unit = okTest {
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
      assertEquals(0, columnsAfterDelete.getArray("columns").size())
    }
  }

  @Test
  def testFillMultilanguageCell(): Unit = okTest {
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
  def testEmptyMultilanguageCell(): Unit = okTest {

    val exceptedJson = Json.fromObjectString(
      """
        |{
        |  "status" : "ok",
        |  "value" : {
        |    "de_DE" : null
        |  }
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
  def testFillMultilanguageRow(): Unit = okTest {
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

    val exceptedJson = Json.fromObjectString(
      """
        |{
        |  "status" : "ok",
        |  "id" : 1,
        |  "values" : [ {
        |    "de_DE" : "Hallo, Welt!",
        |    "en_US" : "Hello, World!"
        |  } ]
        |}
      """.stripMargin)

    for {
      (tableId, columnId) <- createTableWithMultilanguageColumn()

      rowId <- sendRequest("POST", s"/tables/$tableId/rows", valuesRow) map (_.getArray("rows").get[JsonObject](0).getLong("id"))

      row <- sendRequest("GET", s"/tables/$tableId/rows/$rowId")
    } yield {
      assertEquals(exceptedJson, row)
    }
  }

  private def createTableWithMultilanguageColumn(): Future[(Long, Long)] = {
    val createMultilanguageColumn = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1", "multilanguage" -> true)))

    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "Multi Language")) map (_.getLong("id"))
      columnId <- sendRequest("POST", s"/tables/$tableId/columns", createMultilanguageColumn) map (_.getArray("columns").get[JsonObject](0).getLong("id"))
    } yield {
      (tableId, columnId)
    }
  }
}
