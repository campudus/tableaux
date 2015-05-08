package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.http.HttpClient
import org.vertx.scala.core.json.JsonObject

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
      columnId <- sendRequestWithJson("POST", postLinkCol, "/tables/1/columns") map { _.getArray("columns").get[JsonObject](0).getLong("id") }
      // add row 1 to table 2
      rowId1 <- sendRequestWithJson("POST", valuesRow("Lala"), "/tables/2/rows") map { _.getArray("rows").get[JsonObject](0).getInteger("id")}
      // add row 2 to table 2
      rowId2 <- sendRequestWithJson("POST", valuesRow("Lulu"), "/tables/2/rows") map { _.getArray("rows").get[JsonObject](0).getInteger("id")}
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
          Json.obj("value" -> Json.arr(
              Json.obj("id" -> rowId1, "value" -> "Lala"),
              Json.obj("id" -> rowId2, "value" -> "Lulu")
            )
          )
        )
      )

      assertEquals(expectedJson2.getArray("rows").get[JsonObject](0).getArray("value"), linkValue.getArray("rows").get[JsonObject](0).getArray("value"))
    }
  }

  private def setupTables(): Future[Seq[Long]] = for {
    id1 <- setupDefaultTable()
    id2 <- setupDefaultTable("Test Table 2")
  } yield List(id1, id2)
}