package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.{Json, JsonObject}
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Future

class LinkTest extends TableauxTestBase {

  val postLinkCol = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "fromColumn" -> 1, "toTable" -> 2, "toColumn" -> 1)))

  @Test
  def getLinkColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2, "toColumn" -> 1, "ordering" -> 3)))

    for {
      tables <- setupTables()
      _ <- sendRequestWithJson("POST", postLinkCol, "/tables/1/columns")
      test <- sendRequest("GET", "/tables/1/columns/3")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    for {
      tables <- setupTables()
      test <- sendRequestWithJson("POST", postLinkCol, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumnWithOrdering(): Unit = okTest {
    val postLinkColWithOrd = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "fromColumn" -> 1, "toTable" -> 2, "toColumn" -> 1, "ordering" -> 5)))
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 5)))

    for {
      tables <- setupTables()
      test <- sendRequestWithJson("POST", postLinkColWithOrd, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def fillSingleLinkCell(): Unit = okTest {
    val valuesRow = { c: String =>
      Json.obj("columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)), "rows" -> Json.arr(Json.obj("values" -> Json.arr(c, 2))))
    }

    val fillLinkCellJson = { c: Integer =>
      Json.obj("cells" -> Json.arr(Json.obj("value" -> Json.arr(1, c))))
    }

    val expectedJson = Json.obj("status" -> "ok")

    for {
      tables <- setupTables()
      // create link column
      columnId <- sendRequestWithJson("POST", postLinkCol, "/tables/1/columns") map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }
      // add row 1 to table 2
      rowId1 <- sendRequestWithJson("POST", valuesRow("Lala"), "/tables/2/rows") map {
        _.getArray("rows").get[JsonObject](0).getInteger("id")
      }
      // add row 2 to table 2
      rowId2 <- sendRequestWithJson("POST", valuesRow("Lulu"), "/tables/2/rows") map {
        _.getArray("rows").get[JsonObject](0).getInteger("id")
      }
      // add link 1
      addLink1 <- sendRequestWithJson("POST", fillLinkCellJson(rowId1), s"/tables/1/columns/$columnId/rows/1")
      // add link 2
      addLink2 <- sendRequestWithJson("POST", fillLinkCellJson(rowId2), s"/tables/1/columns/$columnId/rows/1")
      // get link value (so it's a value from table 2 shown in table 1)
      linkValue <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(expectedJson, addLink1)
      assertEquals(expectedJson, addLink2)

      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "rows" -> Json.arr(
          Json.obj(
            "value" -> Json.arr(
              Json.obj("id" -> rowId1, "value" -> "Lala"),
              Json.obj("id" -> rowId2, "value" -> "Lulu")
            )
          )
        )
      )

      assertEquals(expectedJson2.getArray("rows").get[JsonObject](0).getArray("value"), linkValue.getArray("rows").get[JsonObject](0).getArray("value"))
    }
  }

  @Test
  def retrieveLinkValuesFromLinkedTable(): Unit = okTest {
    val valuesRow = { c: String =>
      Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(c, 2)))
      )
    }

    val expectedJsonOk = Json.obj("status" -> "ok")

    val linkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "fromColumn" -> 1,
          "toTable" -> 2,
          "toColumn" -> 2
        )
      )
    )

    val fillLinkCellJson = { (from: Integer, to: Integer) =>
      Json.obj("cells" -> Json.arr(Json.obj("value" -> Json.arr(from, to))))
    }

    for {
      // setup two tables
      tables <- setupTables()

      // create link column
      linkColumnId <- sendRequestWithJson("POST", linkColumn, "/tables/1/columns") map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      // add row 1 to table 1
      table1RowId1 <- sendRequestWithJson("POST", valuesRow("table1RowId1"), "/tables/1/rows") map {
        _.getArray("rows").get[JsonObject](0).getInteger("id")
      }
      // add row 2 to table 1
      table1RowId2 <- sendRequestWithJson("POST", valuesRow("table1RowId2"), "/tables/1/rows") map {
        _.getArray("rows").get[JsonObject](0).getInteger("id")
      }
      // add row 1 to table 2
      table2RowId1 <- sendRequestWithJson("POST", valuesRow("table2RowId1"), "/tables/2/rows") map {
        _.getArray("rows").get[JsonObject](0).getInteger("id")
      }
      // add row 2 to table 2
      table2RowId2 <- sendRequestWithJson("POST", valuesRow("table2RowId2"), "/tables/2/rows") map {
        _.getArray("rows").get[JsonObject](0).getInteger("id")
      }

      // add link 1 (table 1 to table 2)
      addLink1 <- sendRequestWithJson("POST", fillLinkCellJson(table1RowId1, table2RowId1), s"/tables/1/columns/$linkColumnId/rows/$table1RowId1")
      // add link 2
      addLink2 <- sendRequestWithJson("POST", fillLinkCellJson(table1RowId1, table2RowId2), s"/tables/1/columns/$linkColumnId/rows/$table1RowId1")
      // add link 3
      addLink3 <- sendRequestWithJson("POST", fillLinkCellJson(table1RowId2, table2RowId1), s"/tables/2/columns/$linkColumnId/rows/$table2RowId1")

      // get link values (so it's a value from table 2 shown in table 1)
      linkValueForTable1 <- sendRequest("GET", s"/tables/1/rows/$table1RowId1")

      // get link values (so it's a value from table 1 shown in table 2)
      linkValueForTable2 <- sendRequest("GET", s"/tables/2/rows/$table2RowId1")
    } yield {
      assertEquals(expectedJsonOk, addLink1)
      assertEquals(expectedJsonOk, addLink2)

      val expectedJsonForResult1 = Json.obj(
        "status" -> "ok",
        "rows" -> Json.arr(
          Json.obj(
            "id" -> table1RowId1,
            "values" -> Json.arr(
              "table1RowId1",
              2,
              Json.arr(
                Json.obj("id" -> table2RowId1, "value" -> 2),
                Json.obj("id" -> table2RowId2, "value" -> 2)
              )
            )
          )
        )
      )

      val expectedJsonForResult2 = Json.obj(
        "status" -> "ok",
        "rows" -> Json.arr(
          Json.obj(
            "id" -> table2RowId1,
            "values" -> Json.arr(
              "table2RowId1",
              2,
              Json.arr(
                Json.obj("id" -> table1RowId1, "value" -> "table1RowId1"),
                Json.obj("id" -> table1RowId2, "value" -> "table1RowId2")
              )
            )
          )
        )
      )

      assertEquals(expectedJsonForResult1, linkValueForTable1)

      assertEquals(expectedJsonForResult2, linkValueForTable2)
    }
  }

  private def setupTables(): Future[Seq[Long]] = for {
    id1 <- setupDefaultTable()
    id2 <- setupDefaultTable("Test Table 2")
  } yield List(id1, id2)
}