package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class LinkTest extends TableauxTestBase {

  val postLinkCol = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2)))
  val postSingleDirectionLinkCol = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2, "singleDirection" -> true)))

  @Test
  def retrieveLinkColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 3,
      "name" -> "Test Link 1",
      "kind" -> "link",
      "multilanguage" -> false,
      "toTable" -> 2,
      "toColumn" -> Json.obj(
        "id" -> 1,
        "ordering" -> 1,
        "name" -> "Test Column 1",
        "kind" -> "text",
        "multilanguage" -> false,
        "identifier" -> true
      ),
      "ordering" -> 3,
      "identifier" -> false
    )

    for {
      tables <- setupTwoTables()
      _ <- sendRequest("POST", "/tables/1/columns", postLinkCol)
      test <- sendRequest("GET", "/tables/1/columns/3")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumn(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    for {
      tables <- setupTwoTables()
      test <- sendRequest("POST", "/tables/1/columns", postLinkCol)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumnWithOrdering(implicit c: TestContext): Unit = okTest {
    val postLinkColWithOrd = Json.obj("columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2, "ordering" -> 5)))
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 5)))

    for {
      tables <- setupTwoTables()
      test <- sendRequest("POST", "/tables/1/columns", postLinkColWithOrd)
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumnWithToName(implicit c: TestContext): Unit = okTest {
    val postLinkColWithOrd = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toName" -> "Backlink",
          "toTable" -> 2,
          "ordering" -> 5
        )
      )
    )
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 5)))

    for {
      tables <- setupTwoTables()
      createLink <- sendRequest("POST", "/tables/1/columns", postLinkColWithOrd)
      retrieveLinkColumn <- sendRequest("GET", s"/tables/2/columns/3")
    } yield {
      assertEquals(expectedJson, createLink)
      assertEquals("Backlink", retrieveLinkColumn.getString("name"))
    }
  }

  @Test
  def createLinkColumnSingleDirection(implicit c: TestContext): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 3, "ordering" -> 3)))

    for {
      tables <- setupTwoTables()
      test <- sendRequest("POST", "/tables/1/columns", postSingleDirectionLinkCol)

      columnsA <- sendRequest("GET", s"/tables/${tables.head}/columns")
      columnsB <- sendRequest("GET", s"/tables/${tables.last}/columns")
    } yield {
      assertEquals(expectedJson, test)

      logger.info(columnsA.encode())
      logger.info(columnsB.encode())

      assertEquals(3, columnsA.getArray("columns").size())
      assertEquals(2, columnsB.getArray("columns").size())
    }
  }

  @Test
  def fillAndRetrieveLinkCell(implicit c: TestContext): Unit = okTest {
    def valuesRow(c: String) =
      Json.obj(
        "columns" -> Json.arr(
          Json.obj("id" -> 1),
          Json.obj("id" -> 2)
        ),
        "rows" -> Json.arr(
          Json.obj("values" -> Json.arr(c, 2))
        ))

    def fillLinkCellJson(c: Integer) = Json.obj("value" -> Json.obj("to" -> c))

    val expectedJson = Json.obj("status" -> "ok")

    for {
      tables <- setupTwoTables()
      // create link column
      postResult <- sendRequest("POST", "/tables/1/columns", postLinkCol)
      columnId = postResult.getArray("columns").get[JsonObject](0).getLong("id")

      // add row 1 to table 2
      postResult <- sendRequest("POST", "/tables/2/rows", valuesRow("Lala"))
      rowId1 = postResult.getArray("rows").get[JsonObject](0).getInteger("id")

      // add row 2 to table 2
      postResult <- sendRequest("POST", "/tables/2/rows", valuesRow("Lulu"))
      rowId2 = postResult.getArray("rows").get[JsonObject](0).getInteger("id")

      // add link 1
      addLink1 <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", fillLinkCellJson(rowId1))
      // add link 2
      addLink2 <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", fillLinkCellJson(rowId2))

      // get link value (so it's a value from table 2 shown in table 1)
      linkValue <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
    } yield {
      assertEquals(expectedJson, addLink1)
      assertEquals(expectedJson, addLink2)

      val expectedJson2 = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          Json.obj("id" -> rowId1, "value" -> "Lala"),
          Json.obj("id" -> rowId2, "value" -> "Lulu")
        )
      )

      assertEquals(expectedJson2, linkValue)
    }
  }

  @Test
  def retrieveLinkValuesFromLinkedTable(implicit c: TestContext): Unit = okTest {
    def valuesRow(c: String) = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr(c, 2)))
    )

    val expectedJsonOk = Json.obj("status" -> "ok")

    val linkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )

    def fillLinkCellJson(from: Number, to: Number) = Json.obj("value" -> Json.obj("to" -> to))

    def addRow(tableId: Long, values: JsonObject): Future[Number] = for {
      res <- sendRequest("POST", s"/tables/$tableId/rows", values)
      table1RowId1 <- Future.apply(res.getArray("rows").get[JsonObject](0).getNumber("id"))
    } yield table1RowId1

    for {
      tables <- setupTwoTables()

      // create link column
      res <- sendRequest("POST", "/tables/1/columns", linkColumn)
      linkColumnId = res.getArray("columns").get[JsonObject](0).getNumber("id")

      // add rows to tables
      table1RowId1 <- addRow(1, valuesRow("table1RowId1"))
      table1RowId2 <- addRow(1, valuesRow("table1RowId2"))
      table2RowId1 <- addRow(2, valuesRow("table2RowId1"))
      table2RowId2 <- addRow(2, valuesRow("table2RowId2"))

      // add link 1 (table 1 to table 2)
      addLink1 <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/$table1RowId1", fillLinkCellJson(table1RowId1, table2RowId2))
      // add link 2
      addLink2 <- sendRequest("POST", s"/tables/2/columns/$linkColumnId/rows/$table2RowId1", fillLinkCellJson(table2RowId1, table1RowId2))

      // get link values (so it's a value from table 2 shown in table 1)
      linkValueForTable1 <- sendRequest("GET", s"/tables/1/rows/$table1RowId1")

      // get link values (so it's a value from table 1 shown in table 2)
      linkValueForTable2 <- sendRequest("GET", s"/tables/2/rows/$table2RowId1")
    } yield {
      assertEquals(expectedJsonOk, addLink1)
      assertEquals(expectedJsonOk, addLink2)

      val expectedJsonForResult1 = Json.obj(
        "status" -> "ok",
        "id" -> table1RowId1,
        "values" -> Json.arr(
          "table1RowId1",
          2,
          Json.arr(
            Json.obj("id" -> table2RowId2, "value" -> "table2RowId2")
          )
        )
      )

      val expectedJsonForResult2 = Json.obj(
        "status" -> "ok",
        "id" -> table2RowId1,
        "values" -> Json.arr(
          "table2RowId1",
          2,
          Json.arr(
            Json.obj("id" -> table1RowId2, "value" -> "table1RowId2")
          )
        )
      )

      assertEquals(expectedJsonForResult1, linkValueForTable1)

      assertEquals(expectedJsonForResult2, linkValueForTable2)
    }
  }

  private def setupTwoTablesWithEmptyLinks(): Future[Number] = {
    val linkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )

    def addRow(tableId: Long, values: JsonObject): Future[Number] = for {
      res <- sendRequest("POST", s"/tables/$tableId/rows", values)
      table1RowId1 <- Future.apply(res.getArray("rows").get[JsonObject](0).getNumber("id"))
    } yield table1RowId1

    def valuesRow(c: String) = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr(c, 2)))
    )

    for {
      tables <- setupTwoTables()

      // create link column
      res <- sendRequest("POST", "/tables/1/columns", linkColumn)
      linkColumnId <- Future.apply(res.getArray("columns").get[JsonObject](0).getNumber("id"))

      // add rows to tables
      table1RowId1 <- addRow(1, valuesRow("table1RowId1"))
      table1RowId2 <- addRow(1, valuesRow("table1RowId2"))
      table2RowId1 <- addRow(2, valuesRow("table2RowId1"))
      table2RowId2 <- addRow(2, valuesRow("table2RowId2"))
    } yield linkColumnId
  }

  @Test
  def putLinkValues(implicit c: TestContext): Unit = okTest {

    val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      resPut <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putLinks)
      // check first table for the link (links to t2, r1 and t2, r2)
      resGet1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
      // check first table for the link (links to nothing)
      resGet2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/2")
      // check second table for the link (links to t1, r1)
      resGet3 <- sendRequest("GET", s"/tables/2/columns/$linkColumnId/rows/1")
      // check second table for the link (links to t1, r1)
      resGet4 <- sendRequest("GET", s"/tables/2/columns/$linkColumnId/rows/2")
    } yield {
      val expected1 = Json.obj("status" -> "ok", "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> "table2row1"),
        Json.obj("id" -> 2, "value" -> "table2row2")
      )
      )
      val expected2 = Json.obj("status" -> "ok", "value" -> Json.arr())
      val expected3 = Json.obj("status" -> "ok", "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> "table1row1")
      )
      )
      val expected4 = Json.obj("status" -> "ok", "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> "table1row1")
      )
      )

      assertEquals(Json.obj("status" -> "ok"), resPut)
      assertEquals(expected1, resGet1)
      assertEquals(expected2, resGet2)
      assertEquals(expected3, resGet3)
      assertEquals(expected4, resGet4)
    }
  }

  @Test
  def putLinkValuesOnMultiLanguageColumns(implicit c: TestContext): Unit = okTest {
    val postLinkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )
    val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      (tableId1, columnIds1, table1rowId1) <- createFullTableWithMultilanguageColumns("Table 1")
      (tableId2, columnIds2, table2rowId1) <- createFullTableWithMultilanguageColumns("Table 2")
      linkColumn <- sendRequest("POST", s"/tables/$tableId1/columns", postLinkColumn)
      linkColumnId = linkColumn.getArray("columns").get[JsonObject](0).getNumber("id")

      resPut <- sendRequest("PUT", s"/tables/$tableId1/columns/$linkColumnId/rows/1", putLinks)
      // check first table for the link (links to t2, r1, c4)
      resGet1 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/1")
      // check first table for the link (links to nothing)
      resGet2 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/2")
      // check second table for the link (links to t1, r1, c1)
      resGet3 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId/rows/1")
      // check second table for the link (links to t1, r1, c1)
      resGet4 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId/rows/2")
    } yield {
      val expected1 = Json.obj("status" -> "ok", "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> Json.obj("en_US" -> "Hello, Table 2 World!", "de_DE" -> "Hallo, Table 2 Welt!")),
        Json.obj("id" -> 2, "value" -> Json.obj("en_US" -> "Hello, Table 2 World2!", "de_DE" -> "Hallo, Table 2 Welt2!"))
      ))
      val expected2 = Json.obj("status" -> "ok", "value" -> Json.arr())
      val expected3 = Json.obj("status" -> "ok", "value" ->
        Json.arr(Json.obj("id" -> 1, "value" -> Json.obj(
          "de_DE" -> "Hallo, Table 1 Welt!",
          "en_US" -> "Hello, Table 1 World!"
        )))
      )
      val expected4 = expected3

      assertEquals(Json.obj("status" -> "ok"), resPut)
      assertEquals(expected1, resGet1)
      assertEquals(expected2, resGet2)
      assertEquals(expected3, resGet3)
      assertEquals(expected4, resGet4)
    }
  }

  @Test
  def postLinkValueOnMultiLanguageColumns(implicit c: TestContext): Unit = okTest {
    val postLinkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )
    val postLinkValue = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      (tableId1, columnIds1, table1rowIds) <- createFullTableWithMultilanguageColumns("Table 1")
      (tableId2, columnIds2, table2rowIds) <- createFullTableWithMultilanguageColumns("Table 2")
      linkColumn <- sendRequest("POST", s"/tables/$tableId1/columns", postLinkColumn)
      linkColumnId = linkColumn.getArray("columns").get[JsonObject](0).getNumber("id")

      resPost <- sendRequest("POST", s"/tables/$tableId1/columns/$linkColumnId/rows/${table1rowIds.head}", postLinkValue)
      // check first table for the link (links to t2, r1 and r2, c4)
      resGet1 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/${table1rowIds.head}")
      // check first table for the link in row 2 (links to nothing)
      resGet2 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/${table1rowIds.drop(1).head}")
      // check second table for the link (links to t1, r1, c1)
      resGet3 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId/rows/${table2rowIds.head}")
      // check second table for the link in row 2 (links to t1, r1, c1)
      resGet4 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId/rows/${table2rowIds.drop(1).head}")
    } yield {
      val expected1 = Json.obj("status" -> "ok", "value" -> Json.arr(
        Json.obj("id" -> 1, "value" -> Json.obj("en_US" -> "Hello, Table 2 World!", "de_DE" -> "Hallo, Table 2 Welt!")),
        Json.obj("id" -> 2, "value" -> Json.obj("en_US" -> "Hello, Table 2 World2!", "de_DE" -> "Hallo, Table 2 Welt2!"))
      ))
      val expected2 = Json.obj("status" -> "ok", "value" -> Json.arr())
      val expected3 = Json.obj("status" -> "ok", "value" ->
        Json.arr(Json.obj("id" -> 1, "value" -> Json.obj(
          "de_DE" -> "Hallo, Table 1 Welt!",
          "en_US" -> "Hello, Table 1 World!"
        )))
      )
      val expected4 = expected3

      assertEquals(Json.obj("status" -> "ok"), resPost)
      assertEquals(expected1, resGet1)
      assertEquals(expected2, resGet2)
      assertEquals(expected3, resGet3)
      assertEquals(expected4, resGet4)
    }
  }

  @Test
  def deleteAllLinkValues(implicit c: TestContext): Unit = okTest {
    val putTwoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))
    val putOneLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1)))
    val putZeroLinks = Json.obj("value" -> Json.obj("values" -> Json.arr()))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      resPut1 <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)
      // check first table for the link (links to t2, r1 and t2, r2)
      resGet1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")

      //remove link to t2, r2
      resPut2 <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putOneLinks)
      // check first table for the link (links to t2, r1)
      resGet2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")

      //remove link to t2, r1
      resPut3 <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putZeroLinks)
      // check first table for the link (no link values anymore)
      resGet3 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      val expected1 = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          Json.obj("id" -> 1, "value" -> "table2row1"),
          Json.obj("id" -> 2, "value" -> "table2row2")
        )
      )

      val expected2 = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          Json.obj("id" -> 1, "value" -> "table2row1")
        )
      )

      val expected3 = Json.obj("status" -> "ok", "value" -> Json.arr())

      assertEquals(Json.obj("status" -> "ok"), resPut1)
      assertEquals(Json.obj("status" -> "ok"), resPut2)
      assertEquals(Json.obj("status" -> "ok"), resPut3)

      assertEquals(expected1, resGet1)
      assertEquals(expected2, resGet2)
      assertEquals(expected3, resGet3)
    }
  }

  @Test
  def invalidPutLinkValueToMissing(implicit c: TestContext): Unit = {
    // Should contain a "to" value
    invalidJsonForLink(Json.obj("value" -> Json.obj("invalid" -> "no to")))
  }

  @Test
  def invalidPutLinkValueToString(implicit c: TestContext): Unit = {
    // Should contain a "to" value that is an integer
    invalidJsonForLink(Json.obj("value" -> Json.obj("to" -> "hello")))
  }

  @Test
  def invalidPutLinkValuesStrings(implicit c: TestContext): Unit = {
    // Should contain values that is an integer
    invalidJsonForLink(Json.obj("value" -> Json.obj("values" -> Json.arr("hello"))))
  }

  private def invalidJsonForLink(input: JsonObject)(implicit c: TestContext) = exceptionTest("error.json.link-value") {
    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      resPut <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", input)
    } yield resPut
  }

  @Test
  def retrieveEmptyLinkValue(implicit c: TestContext): Unit = okTest {
    val linkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 2
        )
      )
    )

    for {
      tables <- setupTwoTables()

      // create link column
      linkColumnId <- sendRequest("POST", "/tables/1/columns", linkColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      // add empty row
      emptyRow <- sendRequest("POST", "/tables/1/rows") map (_.getInteger("id"))

      // get empty link values
      emptyLinkValue <- sendRequest("GET", s"/tables/1/rows/$emptyRow")
    } yield {
      val expectedJson = Json.obj(
        "status" -> "ok",
        "id" -> emptyRow,
        "values" -> Json.arr(null, null, Json.arr())
      )

      assertEquals(expectedJson, emptyLinkValue)
    }
  }

  @Test
  def createSelfLink(implicit c: TestContext): Unit = okTest {
    val linkColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "toTable" -> 1
        )
      )
    )

    def addRow(tableId: Long, values: JsonObject): Future[Number] = for {
      res <- sendRequest("POST", s"/tables/$tableId/rows", values)
      table1RowId1 = res.getArray("rows").get[JsonObject](0).getNumber("id")
    } yield table1RowId1

    def valuesRow(c: String) = Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr(c)))
    )

    def putLinks(arr: JsonArray) = Json.obj("value" -> Json.obj("values" -> arr))

    val postTable = Json.obj("name" -> "Test Table")
    val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1", "identifier" -> true)))

    for {
      tableId <- sendRequest("POST", "/tables", postTable) map { js => js.getLong("id") }
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createStringColumnJson)

      // create link column
      linkColumnId <- sendRequest("POST", "/tables/1/columns", linkColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      // add rows
      row1 <- addRow(1, valuesRow("blub 1"))
      row2 <- addRow(1, valuesRow("blub 2"))
      row3 <- addRow(1, valuesRow("blub 3"))

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/$row1", putLinks(Json.arr(row2)))
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/$row2", putLinks(Json.arr(row3)))
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/$row3", putLinks(Json.arr(row1)))

      // get link values
      rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows")

      // get column
      columns <- sendRequest("GET", s"/tables/1/columns") map { js => js.getJsonArray("columns") }
    } yield {
      val expectedJson = Json.fromObjectString(
        s"""
           |{
           |  "status": "ok",
           |  "page": {
           |    "offset": null,
           |    "limit": null,
           |    "totalSize": 3
           |  },
           |  "rows": [
           |    {"id": 1,"values": [[{"id": 2,"value": "blub 2"}]]},
           |    {"id": 2,"values": [[{"id": 3,"value": "blub 3"}]]},
           |    {"id": 3,"values": [[{"id": 1,"value": "blub 1"}]]}
           |  ]
           |}
         """.stripMargin)

      assertEquals(expectedJson, rows)

      // text column, and the self-link column
      assertEquals(2, columns.size())
    }
  }

  private def setupTwoTables(): Future[Seq[Long]] = for {
    id1 <- setupDefaultTable()
    id2 <- setupDefaultTable("Test Table 2", 2)
  } yield List(id1, id2)
}