package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future
import scala.util.Random

sealed trait LinkTestBase extends TableauxTestBase {

  protected def postLinkCol(toTableId: TableId, name: String = "Test Link 1") = {
    Json.obj("columns" -> Json.arr(Json.obj("name" -> name, "kind" -> "link", "toTable" -> toTableId)))
  }

  protected def postSingleDirectionLinkCol(toTableId: TableId, name: String = "Test Link 1") = {
    Json.obj(
      "columns" -> Json.arr(
        Json.obj("name" -> name, "kind" -> "link", "toTable" -> toTableId, "singleDirection" -> true)))
  }

  protected def postDefaultTableRow(string: String, number: Number) = {
    Json.obj(
      "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
      "rows" -> Json.arr(Json.obj("values" -> Json.arr(string, number)))
    )
  }

  protected def addRow(tableId: TableId): Future[RowId] = {
    for {
      res <- sendRequest("POST", s"/tables/$tableId/rows")
      rowId = res.getLong("id").toLong
    } yield rowId
  }

  protected def addRow(tableId: TableId, values: JsonObject): Future[RowId] = {
    for {
      res <- sendRequest("POST", s"/tables/$tableId/rows", values)
      rowId = res.getArray("rows").getJsonObject(0).getLong("id").toLong
    } yield rowId
  }

  protected def setupTwoTablesWithEmptyLinks(): Future[ColumnId] = {
    for {
      (tableId, toTableId) <- setupTwoTables()

      // create link column
      linkColumnId <- createLinkColumn(tableId, toTableId, singleDirection = false)

      // add rows to tables
      table1RowId1 <- addRow(tableId, postDefaultTableRow("table1RowId1", 2))
      table1RowId2 <- addRow(tableId, postDefaultTableRow("table1RowId2", 2))
      table1RowId2 <- addRow(tableId, postDefaultTableRow("table1RowId3", 2))

      table2RowId1 <- addRow(toTableId, postDefaultTableRow("table2RowId1", 2))
      table2RowId2 <- addRow(toTableId, postDefaultTableRow("table2RowId2", 2))
      table2RowId2 <- addRow(toTableId, postDefaultTableRow("table2RowId3", 2))

    } yield linkColumnId
  }

  protected def setupTwoTables(): Future[(TableId, TableId)] = {
    for {
      id1 <- createDefaultTable()
      id2 <- createDefaultTable("Test Table 2", 2)
    } yield (id1, id2)
  }

  protected def createLinkColumn(tableId: TableId, toTableId: TableId, singleDirection: Boolean): Future[ColumnId] = {
    val rand = Random.nextInt()

    val json = if (singleDirection) {
      postSingleDirectionLinkCol(toTableId, s"Link $tableId, $toTableId, $singleDirection, $rand")
    } else {
      postLinkCol(toTableId, s"Link $tableId, $toTableId, $singleDirection, $rand")
    }

    sendRequest("POST", s"/tables/$tableId/columns", json)
      .map(_.getJsonArray("columns").getJsonObject(0).getLong("id").toLong)
  }

  protected def putLink(tableId: TableId, columnId: ColumnId, fromRowId: RowId, toRowId: RowId): Future[Unit] = {
    val putLinkJson = Json.obj("value" -> Json.obj("to" -> toRowId))

    sendRequest("POST", s"/tables/$tableId/columns/$columnId/rows/$fromRowId", putLinkJson)
      .map(_ => ())
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkColumnTest extends LinkTestBase {

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
        "identifier" -> true,
        "displayName" -> Json.obj(),
        "description" -> Json.obj()
      ),
      "ordering" -> 3,
      "identifier" -> false,
      "displayName" -> Json.obj(),
      "description" -> Json.obj()
    )

    for {
      tables <- setupTwoTables()
      _ <- sendRequest("POST", "/tables/1/columns", postLinkCol(toTableId = 2))
      test <- sendRequest("GET", "/tables/1/columns/3")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def createLinkColumn(implicit c: TestContext): Unit = {
    okTest{
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 3,
            "ordering" -> 3,
            "name" -> "Test Link 1",
            "kind" -> "link",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "toTable" -> 2,
            "toColumn" -> Json.obj(
              "id" -> 1,
              "ordering" -> 1,
              "kind" -> "text",
              "name" -> "Test Column 1",
              "multilanguage" -> false,
              "identifier" -> true,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
          ))
      )

      for {
        tables <- setupTwoTables()
        test <- sendRequest("POST", "/tables/1/columns", postLinkCol(toTableId = 2))
      } yield {
        assertEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createLinkColumnWithOrdering(implicit c: TestContext): Unit = {
    okTest{
      val postLinkColWithOrd = Json.obj(
        "columns" -> Json.arr(Json.obj("name" -> "Test Link 1", "kind" -> "link", "toTable" -> 2, "ordering" -> 5)))
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 3,
            "ordering" -> 5,
            "name" -> "Test Link 1",
            "kind" -> "link",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "toTable" -> 2,
            "toColumn" -> Json.obj(
              "id" -> 1,
              "ordering" -> 1,
              "kind" -> "text",
              "name" -> "Test Column 1",
              "multilanguage" -> false,
              "identifier" -> true,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
          ))
      )

      for {
        tables <- setupTwoTables()
        test <- sendRequest("POST", "/tables/1/columns", postLinkColWithOrd)
      } yield {
        assertEquals(expectedJson, test)
      }
    }
  }

  @Test
  def createLinkColumnWithToName(implicit c: TestContext): Unit = {
    okTest{
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
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 3,
            "ordering" -> 5,
            "name" -> "Test Link 1",
            "kind" -> "link",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "toTable" -> 2,
            "toColumn" -> Json.obj(
              "id" -> 1,
              "ordering" -> 1,
              "kind" -> "text",
              "name" -> "Test Column 1",
              "multilanguage" -> false,
              "identifier" -> true,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
          ))
      )

      for {
        tables <- setupTwoTables()
        createLink <- sendRequest("POST", "/tables/1/columns", postLinkColWithOrd)
        retrieveLinkColumn <- sendRequest("GET", s"/tables/2/columns/3")
      } yield {
        assertEquals(expectedJson, createLink)
        assertEquals("Backlink", retrieveLinkColumn.getString("name"))
      }
    }
  }

  @Test
  def createLinkColumnSingleDirection(implicit c: TestContext): Unit = {
    okTest{
      val expectedJson = Json.obj(
        "status" -> "ok",
        "columns" -> Json.arr(
          Json.obj(
            "id" -> 3,
            "ordering" -> 3,
            "name" -> "Test Link 1",
            "kind" -> "link",
            "multilanguage" -> false,
            "identifier" -> false,
            "displayName" -> Json.obj(),
            "description" -> Json.obj(),
            "toTable" -> 2,
            "toColumn" -> Json.obj(
              "id" -> 1,
              "ordering" -> 1,
              "kind" -> "text",
              "name" -> "Test Column 1",
              "multilanguage" -> false,
              "identifier" -> true,
              "displayName" -> Json.obj(),
              "description" -> Json.obj()
            )
          ))
      )

      for {
        (table1, table2) <- setupTwoTables()
        test <- sendRequest("POST", "/tables/1/columns", postSingleDirectionLinkCol(toTableId = 2))

        columnsA <- sendRequest("GET", s"/tables/$table1/columns")
        columnsB <- sendRequest("GET", s"/tables/$table2/columns")
      } yield {
        assertEquals(expectedJson, test)

        logger.info(columnsA.encode())
        logger.info(columnsB.encode())

        assertEquals(3, columnsA.getArray("columns").size())
        assertEquals(2, columnsB.getArray("columns").size())
      }
    }
  }

  @Test
  def createLinkColumnToTableWithoutColumns(implicit c: TestContext): Unit = exceptionTest("NOT FOUND") {
    for {
      table1 <- createDefaultTable()
      table2 <- sendRequest("POST", "/tables", Json.obj("name" -> "Empty Table"))

      test <- sendRequest("POST", "/tables/1/columns", postLinkCol(toTableId = 2))
    } yield ()
  }

  @Test
  def createLinkColumnToTableWithoutIdentifier(implicit c: TestContext): Unit = {
    exceptionTest("error.database.missing-identifier"){
      for {
        table1 <- createDefaultTable()
        table2 <- sendRequest("POST", "/tables", Json.obj("name" -> "Empty Table"))

        _ <- sendRequest("POST",
          "/tables/2/columns",
          RequestCreation.Columns().add(RequestCreation.TextCol("Spalte")).getJson)

        test <- sendRequest("POST", "/tables/1/columns", postLinkCol(toTableId = 2))
      } yield ()
    }
  }

  @Test
  def createLinkColumnToTableWithIdentifierAndRemoveIt(implicit c: TestContext): Unit = {
    exceptionTest("error.database.missing-identifier"){
      for {
        table1 <- createDefaultTable()
        table2 <- sendRequest("POST", "/tables", Json.obj("name" -> "Empty Table"))

        _ <- sendRequest(
          "POST",
          "/tables/2/columns",
          RequestCreation.Columns().add(RequestCreation.Identifier(RequestCreation.TextCol("Spalte"))).getJson)

        _ <- sendRequest("POST", "/tables/1/columns", postLinkCol(toTableId = 2))

        _ <- sendRequest("DELETE", "/tables/2/columns/1")

        _ <- sendRequest("GET", "/tables/1/columns")
      } yield ()
    }
  }

  @Test
  def createBiDirectionalLinkColumnAsIdentifier(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable(name = "Test Table 1")
      table2 <- createEmptyDefaultTable(name = "Test Table 2")

      identifierLinkColum = Json.obj("kind" -> "link", "name" -> "link", "toTable" -> table2, "identifier" -> true)
      columns = Json.obj("columns" -> Json.arr(identifierLinkColum))

      _ <- sendRequest("POST", s"/tables/$table1/columns", columns)
      // both tables should only have one identifier column
      _ <- sendRequest("POST", s"/tables/$table1/columns/1", Json.obj("identifier" -> false))

      resultColumns1 <- sendRequest("GET", "/tables/1/columns")
      resultColumns2 <- sendRequest("GET", "/tables/2/columns")
    } yield {
      import com.campudus.tableaux.helper.JsonUtils._

      val columns1 = asCastedList[JsonObject](resultColumns1.getJsonArray("columns")).get
      val columns2 = asCastedList[JsonObject](resultColumns2.getJsonArray("columns")).get

      assertEquals(columns1.size, columns2.size)
      // in table 1 the link column should be an identifier
      assertEquals("link", columns1.head.getString("kind"))
      assertTrue(s"identifier of ${columns1.head} should be true", columns1.head.getBoolean("identifier", false))

      // in table 2 the link column should not be an identifier - otherwise we would have a cycle
      assertEquals("link", columns2(2).getString("kind"))
      assertFalse(s"identifier of ${columns2(2)} should be false", columns2(2).getBoolean("identifier", false))
    }
  }

  @Test
  def createBiDirectionalLinkColumnWithoutDisplayInfos(implicit c: TestContext): Unit = {
    okTest{
      for {
        table1 <- createEmptyDefaultTable(name = "Test Table 1",
          tableNum = 1,
          Some(Json.obj("de" -> "Test Deutsch 1")),
          None)
        table2 <- createEmptyDefaultTable(name = "Test Table 2",
          tableNum = 1,
          Some(Json.obj("de" -> "Test Deutsch 2")),
          None)

        linkColumn = Json.obj("kind" -> "link", "name" -> "link", "toTable" -> table2)
        columns = Json.obj("columns" -> Json.arr(linkColumn))

        createdLinkColumn <- sendRequest("POST", s"/tables/$table1/columns", columns)
        linkColumnId = createdLinkColumn.getJsonArray("columns").getJsonObject(0).getLong("id").toLong

        resultColumns1 <- sendRequest("GET", s"/tables/$table1/columns/$linkColumnId")
        resultColumns2 <- sendRequest("GET", s"/tables/$table2/columns/$linkColumnId")
      } yield {
        assertEquals("link", resultColumns1.getString("name"))
        assertEquals("Test Table 1", resultColumns2.getString("name"))
        assertEquals(Json.obj("de" -> "Test Deutsch 1"), resultColumns2.getJsonObject("displayName"))
      }
    }
  }

  @Test
  def createBiDirectionalLinkColumnWithDisplayInfos(implicit c: TestContext): Unit = {
    okTest{
      for {
        table1 <- createEmptyDefaultTable(name = "Test Table 1",
          tableNum = 1,
          Some(Json.obj("de" -> "Test Deutsch 1")),
          None)
        table2 <- createEmptyDefaultTable(name = "Test Table 2",
          tableNum = 1,
          Some(Json.obj("de" -> "Test Deutsch 2")),
          None)

        linkColumn = Json.obj(
          "kind" -> "link",
          "name" -> "link",
          "toTable" -> table2,
          "toDisplayInfos" -> Json.obj(
            "displayName" -> Json.obj("de" -> "displayName"),
            "description" -> Json.obj("de" -> "description")
          )
        )
        columns = Json.obj("columns" -> Json.arr(linkColumn))

        createdLinkColumn <- sendRequest("POST", s"/tables/$table1/columns", columns)
        linkColumnId = createdLinkColumn.getJsonArray("columns").getJsonObject(0).getLong("id").toLong

        resultColumns1 <- sendRequest("GET", s"/tables/$table1/columns/$linkColumnId")
        resultColumns2 <- sendRequest("GET", s"/tables/$table2/columns/$linkColumnId")
      } yield {
        assertEquals("link", resultColumns1.getString("name"))
        assertEquals("Test Table 1", resultColumns2.getString("name"))
        assertEquals(Json.obj("de" -> "displayName"), resultColumns2.getJsonObject("displayName"))
        assertEquals(Json.obj("de" -> "description"), resultColumns2.getJsonObject("description"))
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkTest extends LinkTestBase {
  import scala.collection.JavaConverters._

  @Test
  def fillAndRetrieveLinkCell(implicit c: TestContext): Unit = {
    okTest{

      def valuesRow(c: String) =
        Json.obj("columns" -> Json.arr(
          Json.obj("id" -> 1),
          Json.obj("id" -> 2)
        ),
          "rows" -> Json.arr(
            Json.obj("values" -> Json.arr(c, 2))
          ))

      def fillLinkCellJson(c: Integer) = Json.obj("value" -> Json.obj("to" -> c))

      for {
        tables <- setupTwoTables()
        // create link column
        postResult <- sendRequest("POST", "/tables/1/columns", postLinkCol(2))
        columnId = postResult.getJsonArray("columns").getJsonObject(0).getLong("id")

        // add row 1 to table 2
        postResult <- sendRequest("POST", "/tables/2/rows", valuesRow("Lala"))
        _ = logger.info(s"postResult add row 1 to table 2 -> ${postResult.encode()}")
        rowId1 = postResult.getJsonArray("rows").getJsonObject(0).getInteger("id")

        // add row 2 to table 2
        postResult <- sendRequest("POST", "/tables/2/rows", valuesRow("Lulu"))
        _ = logger.info(s"postResult add row 2 to table 2 -> ${postResult.encode()}")
        rowId2 = postResult.getJsonArray("rows").getJsonObject(0).getInteger("id")

        // add link 1
        addLink1 <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", fillLinkCellJson(rowId1))
        // add link 2
        addLink2 <- sendRequest("POST", s"/tables/1/columns/$columnId/rows/1", fillLinkCellJson(rowId2))

        // get link value (so it's a value from table 2 shown in table 1)
        linkValue <- sendRequest("GET", s"/tables/1/columns/$columnId/rows/1")
      } yield {
        assertEquals(Json.obj("status" -> "ok", "value" -> Json.arr(Json.obj("id" -> rowId1, "value" -> "Lala"))),
          addLink1)
        assertEquals(Json.obj("status" -> "ok",
          "value" -> Json.arr(Json.obj("id" -> rowId1, "value" -> "Lala"),
            Json.obj("id" -> rowId2, "value" -> "Lulu"))),
          addLink2)

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
  }

  @Test
  def retrieveLinkValuesFromLinkedTable(implicit c: TestContext): Unit = {
    okTest{

      def valuesRow(c: String) = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(c, 2)))
      )

      val linkColumn = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "name" -> "Test Link 1",
            "kind" -> "link",
            "toTable" -> 2
          )
        )
      )

      def fillLinkCellJson(to: Number) = Json.obj("value" -> Json.obj("to" -> to))

      def addRow(tableId: Long, values: JsonObject): Future[Number] =
        for {
          res <- sendRequest("POST", s"/tables/$tableId/rows", values)
          table1RowId1 <- Future.apply(res.getJsonArray("rows").getJsonObject(0).getNumber("id"))
        } yield table1RowId1

      for {
        tables <- setupTwoTables()

        // create link column
        res <- sendRequest("POST", "/tables/1/columns", linkColumn)
        linkColumnId = res.getJsonArray("columns").getJsonObject(0).getNumber("id")

        // add rows to tables
        table1RowId1 <- addRow(1, valuesRow("table1RowId1"))
        table1RowId2 <- addRow(1, valuesRow("table1RowId2"))
        table2RowId1 <- addRow(2, valuesRow("table2RowId1"))
        table2RowId2 <- addRow(2, valuesRow("table2RowId2"))

        // add link 1 (table 1 to table 2)
        addLink1 <- sendRequest("POST",
          s"/tables/1/columns/$linkColumnId/rows/$table1RowId1",
          fillLinkCellJson(table2RowId2))
        // add link 2
        addLink2 <- sendRequest("POST",
          s"/tables/2/columns/$linkColumnId/rows/$table2RowId1",
          fillLinkCellJson(table1RowId2))

        // get link values (so it's a value from table 2 shown in table 1)
        linkValueForTable1 <- sendRequest("GET", s"/tables/1/rows/$table1RowId1")

        // get link values (so it's a value from table 1 shown in table 2)
        linkValueForTable2 <- sendRequest("GET", s"/tables/2/rows/$table2RowId1")
      } yield {
        val expectedLink1 = Json.arr(Json.obj("id" -> table2RowId2, "value" -> "table2RowId2"))
        val expectedLink2 = Json.arr(Json.obj("id" -> table1RowId2, "value" -> "table1RowId2"))

        assertEquals(Json.obj("status" -> "ok", "value" -> expectedLink1), addLink1)
        assertEquals(Json.obj("status" -> "ok", "value" -> expectedLink2), addLink2)

        val expectedJsonForResult1 = Json.obj(
          "status" -> "ok",
          "id" -> table1RowId1,
          "values" -> Json.arr(
            "table1RowId1",
            2,
            expectedLink1
          )
        )

        val expectedJsonForResult2 = Json.obj(
          "status" -> "ok",
          "id" -> table2RowId1,
          "values" -> Json.arr(
            "table2RowId1",
            2,
            expectedLink2
          )
        )

        assertEquals(expectedJsonForResult1, linkValueForTable1)

        assertEquals(expectedJsonForResult2, linkValueForTable2)
      }
    }
  }

  @Test
  def putLinkValues(implicit c: TestContext): Unit = {
    okTest{

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
        val expected1 = Json.obj("status" -> "ok",
          "value" -> Json.arr(
            Json.obj("id" -> 1, "value" -> "table2row1"),
            Json.obj("id" -> 2, "value" -> "table2row2")
          ))
        val expected2 = Json.obj("status" -> "ok", "value" -> Json.arr())
        val expected3 = Json.obj("status" -> "ok",
          "value" -> Json.arr(
            Json.obj("id" -> 1, "value" -> "table1row1")
          ))
        val expected4 = Json.obj("status" -> "ok",
          "value" -> Json.arr(
            Json.obj("id" -> 1, "value" -> "table1row1")
          ))

        assertEquals(expected1, resPut)
        assertEquals(expected1, resGet1)
        assertEquals(expected2, resGet2)
        assertEquals(expected3, resGet3)
        assertEquals(expected4, resGet4)
      }
    }
  }

  @Test
  def putLinkInvalidValue(implicit c: TestContext): Unit = exceptionTest("unprocessable.entity") {
    val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(10)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putLinks)
    } yield ()
  }

  @Test
  def putLinkInvalidValues(implicit c: TestContext): Unit = exceptionTest("unprocessable.entity") {
    val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 12)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putLinks)
    } yield ()
  }

  @Test
  def putLinkValueInInvalidRow(implicit c: TestContext): Unit = exceptionTest("NOT FOUND") {
    val putLinks = Json.obj("value" -> 1)

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/10", putLinks)
    } yield ()
  }

  @Test
  def putLinkValuesOnMultiLanguageColumns(implicit c: TestContext): Unit = {
    okTest{
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
        val expected1 = Json.obj(
          "status" -> "ok",
          "value" -> Json.arr(
            Json.obj("id" -> 1,
              "value" -> Json.obj("en_US" -> "Hello, Table 2 World!", "de_DE" -> "Hallo, Table 2 Welt!")),
            Json.obj("id" -> 2,
              "value" -> Json.obj("en_US" -> "Hello, Table 2 World2!", "de_DE" -> "Hallo, Table 2 Welt2!"))
          )
        )
        val expected2 = Json.obj("status" -> "ok", "value" -> Json.arr())
        val expected3 = Json.obj("status" -> "ok",
          "value" ->
            Json.arr(
              Json.obj("id" -> 1,
                "value" -> Json.obj(
                  "de_DE" -> "Hallo, Table 1 Welt!",
                  "en_US" -> "Hello, Table 1 World!"
                ))))
        val expected4 = expected3

        assertEquals(expected1, resPut)
        assertEquals(expected1, resGet1)
        assertEquals(expected2, resGet2)
        assertEquals(expected3, resGet3)
        assertEquals(expected4, resGet4)
      }
    }
  }

  @Test
  def postLinkValueOnMultiLanguageColumns(implicit c: TestContext): Unit = {
    okTest{
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

        resPost <- sendRequest("POST",
          s"/tables/$tableId1/columns/$linkColumnId/rows/${table1rowIds.head}",
          postLinkValue)
        // check first table for the link (links to t2, r1 and r2, c4)
        resGet1 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/${table1rowIds.head}")
        // check first table for the link in row 2 (links to nothing)
        resGet2 <- sendRequest("GET", s"/tables/$tableId1/columns/$linkColumnId/rows/${table1rowIds.drop(1).head}")
        // check second table for the link (links to t1, r1, c1)
        resGet3 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId/rows/${table2rowIds.head}")
        // check second table for the link in row 2 (links to t1, r1, c1)
        resGet4 <- sendRequest("GET", s"/tables/$tableId2/columns/$linkColumnId/rows/${table2rowIds.drop(1).head}")
      } yield {
        val expected1 = Json.obj(
          "status" -> "ok",
          "value" -> Json.arr(
            Json.obj("id" -> 1,
              "value" -> Json.obj("en_US" -> "Hello, Table 2 World!", "de_DE" -> "Hallo, Table 2 Welt!")),
            Json.obj("id" -> 2,
              "value" -> Json.obj("en_US" -> "Hello, Table 2 World2!", "de_DE" -> "Hallo, Table 2 Welt2!"))
          )
        )
        val expected2 = Json.obj("status" -> "ok", "value" -> Json.arr())
        val expected3 = Json.obj("status" -> "ok",
          "value" ->
            Json.arr(
              Json.obj("id" -> 1,
                "value" -> Json.obj(
                  "de_DE" -> "Hallo, Table 1 Welt!",
                  "en_US" -> "Hello, Table 1 World!"
                ))))
        val expected4 = expected3

        assertEquals(expected1, resPost)
        assertEquals(expected1, resGet1)
        assertEquals(expected2, resGet2)
        assertEquals(expected3, resGet3)
        assertEquals(expected4, resGet4)
      }
    }
  }

  @Test
  def deleteLinkValue(implicit c: TestContext): Unit = okTest {
    val putTwoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      resPut1 <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)
      // check first table for the link (links to t2, r1 and t2, r2)
      resGet1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")

      //remove link to t2, r1
      resDelete2 <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1/link/1")
      // check first table for links left (link to r2 should be left)
      resGet2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")

      //remove link to t2, r2
      resDelete3 <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1/link/2")
      // check first table for links left (no link values anymore)
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
          Json.obj("id" -> 2, "value" -> "table2row2")
        )
      )

      val expected3 = Json.obj("status" -> "ok", "value" -> Json.arr())

      assertEquals(expected1, resPut1)
      assertEquals(expected1, resGet1)

      assertEquals(expected2, resDelete2)
      assertEquals(expected2, resGet2)

      assertEquals(expected3, resDelete3)
      assertEquals(expected3, resGet3)
    }
  }

  @Test
  def tryToDeleteLinkValueOfInvalidColumnType(implicit c: TestContext): Unit = {
    exceptionTest("error.request.column.wrongtype"){
      for {
        (tableId1, _) <- setupTwoTables()

        //try to remove link
        _ <- sendRequest("DELETE", s"/tables/$tableId1/columns/1/rows/1/link/2")
      } yield ()
    }
  }

  @Test
  def putLinkValuesInDifferentWays(implicit c: TestContext): Unit = okTest {
    val oneValue = Json.obj("value" -> 1)
    val objOneValue = Json.obj("value" -> Json.obj("to" -> 2))
    val objTwoValues = Json.obj("value" -> Json.obj("values" -> Json.arr(2, 1)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", oneValue)
      // check first table for the link (links to t2, r1 and t2, r2)
      resGet1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")

      _ <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1")

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", objOneValue)
      // check first table for the link (links to t2, r1 and t2, r2)
      resGet2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")

      _ <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1")

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", objTwoValues)
      // check first table for the link (links to t2, r1 and t2, r2)
      resGet3 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
    } yield {
      val expected1 = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          Json.obj("id" -> 1, "value" -> "table2row1")
        )
      )

      val expected2 = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          Json.obj("id" -> 2, "value" -> "table2row2")
        )
      )

      val expected3 = Json.obj(
        "status" -> "ok",
        "value" -> Json.arr(
          Json.obj("id" -> 2, "value" -> "table2row2"),
          Json.obj("id" -> 1, "value" -> "table2row1")
        )
      )

      assertEquals(expected1, resGet1)
      assertEquals(expected2, resGet2)
      assertEquals(expected3, resGet3)
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

      assertEquals(expected1, resPut1)
      assertEquals(expected2, resPut2)
      assertEquals(expected3, resPut3)

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
  def createSelfLink(implicit c: TestContext): Unit = {
    okTest{
      val linkColumn = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "name" -> "Test Link 1",
            "kind" -> "link",
            "toTable" -> 1
          )
        )
      )

      def addRow(tableId: Long, values: JsonObject): Future[Number] = {
        for {
          res <- sendRequest("POST", s"/tables/$tableId/rows", values)
          rowId = res.getArray("rows").get[JsonObject](0).getNumber("id")
        } yield rowId
      }

      def valuesRow(c: String) = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1)),
        "rows" -> Json.arr(Json.obj("values" -> Json.arr(c)))
      )

      def putLinks(arr: JsonArray) = Json.obj("value" -> Json.obj("values" -> arr))

      val postTable = Json.obj("name" -> "Test Table")
      val createStringColumnJson =
        Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1", "identifier" -> true)))

      for {
        tableId <- sendRequest("POST", "/tables", postTable) map { js =>
          js.getLong("id")
        }
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
        columns <- sendRequest("GET", s"/tables/1/columns") map { js =>
          js.getJsonArray("columns")
        }
      } yield {
        val expectedJson = Json.fromObjectString(
          s"""
{
  "status": "ok",
  "page": {
    "offset": null,
    "limit": null,
    "totalSize": 3
  },
  "rows": [
    {"id": 1,"values": [[{"id": 2,"value": "blub 2"}]]},
    {"id": 2,"values": [[{"id": 3,"value": "blub 3"}]]},
    {"id": 3,"values": [[{"id": 1,"value": "blub 1"}]]}
  ]
}
         """.stripMargin)

        assertEquals(expectedJson, rows)

        // text column, and the self-link column
        assertEquals(2, columns.size())
      }
    }
  }

  @Test
  def retrieveDependentRowsSingleDirection(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable("Table 1", 1)
      row11 <- addRow(table1, postDefaultTableRow("Row 1 in Table 1", 1))
      row12 <- addRow(table1, postDefaultTableRow("Row 2 in Table 1", 2))
      row13 <- addRow(table1, postDefaultTableRow("Row 3 in Table 1", 3))

      table2 <- createEmptyDefaultTable("Table 2", 2)
      row21 <- addRow(table2, postDefaultTableRow("Row 1 in Table 2", 1))
      row22 <- addRow(table2, postDefaultTableRow("Row 2 in Table 2", 2))
      row23 <- addRow(table2, postDefaultTableRow("Row 3 in Table 2", 3))

      table3 <- createEmptyDefaultTable("Table 3", 3)
      row31 <- addRow(table3, postDefaultTableRow("Row 1 in Table 3", 1))
      row32 <- addRow(table3, postDefaultTableRow("Row 2 in Table 3", 2))
      row33 <- addRow(table3, postDefaultTableRow("Row 3 in Table 3", 3))

      linkColumn1From1To2 <- createLinkColumn(table1, table2, singleDirection = true)
      linkColumn2From3To1 <- createLinkColumn(table3, table1, singleDirection = true)

      // Test for link from/to table 1.
      // Point of view doesn't matter.
      _ <- putLink(table1, linkColumn1From1To2, row11, row21)
      _ <- putLink(table1, linkColumn1From1To2, row12, row21)

      _ <- putLink(table3, linkColumn2From3To1, row33, row11)
      _ <- putLink(table3, linkColumn2From3To1, row32, row11)

      result <- sendRequest("GET", s"/tables/$table1/rows/$row11/dependent")
      dependentObjectsOfTable1Row1 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })

      result <- sendRequest("GET", s"/tables/$table2/rows/$row21/dependent")
      dependentObjectsOfTable2Row1 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })

      result <- sendRequest("GET", s"/tables/$table3/rows/$row31/dependent")
      dependentObjectsOfTable3Row1 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })
    } yield {
      val dependentRowsOfTable1Row1 = dependentObjectsOfTable1Row1
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 3
        })
        .map({
          _.getJsonArray("rows")
        })

      val expectedDependentRowsOfTable1Row1 = Json.arr(
        Json.obj(
          "id" -> 3,
          "value" -> "Row 3 in Table 3"
        ),
        Json.obj(
          "id" -> 2,
          "value" -> "Row 2 in Table 3"
        )
      )
      assertEquals(Some(expectedDependentRowsOfTable1Row1), dependentRowsOfTable1Row1)

      val dependentRowsOfTable2Row1 = dependentObjectsOfTable2Row1
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 1
        })
        .map({
          _.getJsonArray("rows")
        })

      val expectedDependentRowsOfTable2Row1 = Json.arr(
        Json.obj(
          "id" -> 1,
          "value" -> "Row 1 in Table 1"
        ),
        Json.obj(
          "id" -> 2,
          "value" -> "Row 2 in Table 1"
        )
      )

      assertEquals(Some(expectedDependentRowsOfTable2Row1), dependentRowsOfTable2Row1)

      assertTrue(dependentObjectsOfTable3Row1.isEmpty)
    }
  }

  @Test
  def retrieveDependentRowsBothDirection(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable("Table 1", 1)
      row11 <- addRow(table1, postDefaultTableRow("Row 1 in Table 1", 1))
      row12 <- addRow(table1, postDefaultTableRow("Row 2 in Table 1", 2))
      row13 <- addRow(table1, postDefaultTableRow("Row 3 in Table 1", 3))

      table2 <- createEmptyDefaultTable("Table 2", 2)
      row21 <- addRow(table2, postDefaultTableRow("Row 1 in Table 2", 1))
      row22 <- addRow(table2, postDefaultTableRow("Row 2 in Table 2", 2))
      row23 <- addRow(table2, postDefaultTableRow("Row 3 in Table 2", 3))

      table3 <- createEmptyDefaultTable("Table 3", 3)
      row31 <- addRow(table3, postDefaultTableRow("Row 1 in Table 3", 1))
      row32 <- addRow(table3, postDefaultTableRow("Row 2 in Table 3", 2))
      row33 <- addRow(table3, postDefaultTableRow("Row 3 in Table 3", 3))

      linkColumn1From1To2 <- createLinkColumn(table1, table2, singleDirection = false)
      linkColumn2From1To3 <- createLinkColumn(table1, table3, singleDirection = false)

      _ <- putLink(table1, linkColumn1From1To2, row11, row21)
      _ <- putLink(table1, linkColumn1From1To2, row13, row21)
      _ <- putLink(table1, linkColumn1From1To2, row11, row23)

      _ <- putLink(table1, linkColumn2From1To3, row11, row32)
      _ <- putLink(table1, linkColumn2From1To3, row12, row32)
      _ <- putLink(table1, linkColumn2From1To3, row11, row33)

      result <- sendRequest("GET", s"/tables/$table1/rows/$row11/dependent")
      dependentRows11 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })

      result <- sendRequest("GET", s"/tables/$table2/rows/$row21/dependent")
      dependentRows21 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })

      result <- sendRequest("GET", s"/tables/$table3/rows/$row32/dependent")
      dependentRows32 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })
    } yield {
      val dependentRows11OfTable2 = dependentRows11
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 2
        })
        .map({
          _.getJsonArray("rows")
        })

      val dependentRows11OfTable3 = dependentRows11
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 3
        })
        .map({
          _.getJsonArray("rows")
        })

      val expectedDependentRows11OfTable2 = Json.arr(
        Json.obj(
          "id" -> 1,
          "value" -> "Row 1 in Table 2"
        ),
        Json.obj(
          "id" -> 3,
          "value" -> "Row 3 in Table 2"
        )
      )

      val expectedDependentRows11OfTable3 = Json.arr(
        Json.obj(
          "id" -> 2,
          "value" -> "Row 2 in Table 3"
        ),
        Json.obj(
          "id" -> 3,
          "value" -> "Row 3 in Table 3"
        )
      )

      assertEquals(Some(expectedDependentRows11OfTable2), dependentRows11OfTable2)
      assertEquals(Some(expectedDependentRows11OfTable3), dependentRows11OfTable3)

      val dependentRows21OfTable1 = dependentRows21
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 1
        })
        .map({
          _.getJsonArray("rows")
        })

      val expectedDependentRows21OfTable2 = Json.arr(
        Json.obj(
          "id" -> 1,
          "value" -> "Row 1 in Table 1"
        ),
        Json.obj(
          "id" -> 3,
          "value" -> "Row 3 in Table 1"
        )
      )

      assertEquals(Some(expectedDependentRows21OfTable2), dependentRows21OfTable1)

      val dependentRows32OfTable1 = dependentRows32
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 1
        })
        .map({
          _.getJsonArray("rows")
        })

      val expectedDependentRows32OfTable1 = Json.arr(
        Json.obj(
          "id" -> 1,
          "value" -> "Row 1 in Table 1"
        ),
        Json.obj(
          "id" -> 2,
          "value" -> "Row 2 in Table 1"
        )
      )

      assertEquals(Some(expectedDependentRows32OfTable1), dependentRows32OfTable1)
    }
  }

  @Test
  def retrieveDependentRowsWithTwoLinkColumns(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable("Table 1", 1)
      row11 <- addRow(table1, postDefaultTableRow("Row 1 in Table 1", 1))
      row12 <- addRow(table1, postDefaultTableRow("Row 2 in Table 1", 2))
      row13 <- addRow(table1, postDefaultTableRow("Row 3 in Table 1", 3))

      table2 <- createEmptyDefaultTable("Table 2", 2)
      row21 <- addRow(table2, postDefaultTableRow("Row 1 in Table 2", 1))
      row22 <- addRow(table2, postDefaultTableRow("Row 2 in Table 2", 2))
      row23 <- addRow(table2, postDefaultTableRow("Row 3 in Table 2", 3))

      linkColumn1From1To2 <- createLinkColumn(table2, table1, singleDirection = true)
      linkColumn2From1To2 <- createLinkColumn(table2, table1, singleDirection = true)

      _ <- putLink(table2, linkColumn1From1To2, row21, row11)
      _ <- putLink(table2, linkColumn1From1To2, row23, row11)
      _ <- putLink(table2, linkColumn1From1To2, row22, row11)

      _ <- putLink(table2, linkColumn2From1To2, row23, row11)
      _ <- putLink(table2, linkColumn2From1To2, row21, row11)
      _ <- putLink(table2, linkColumn2From1To2, row22, row11)

      result <- sendRequest("GET", s"/tables/$table1/rows/$row11/dependent")
      dependentRowObjects = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })
    } yield {
      val dependentRows = dependentRowObjects
        .find({
          case o: JsonObject =>
            o.getJsonObject("table").getLong("id") == 2
        })
        .map({
          _.getJsonArray("rows")
        })

      val expectedDependentRows = Json.arr(
        Json.obj(
          "id" -> 1,
          "value" -> "Row 1 in Table 2"
        ),
        Json.obj(
          "id" -> 3,
          "value" -> "Row 3 in Table 2"
        ),
        Json.obj(
          "id" -> 2,
          "value" -> "Row 2 in Table 2"
        )
      )

      assertEquals(Some(expectedDependentRows), dependentRows)
    }
  }

  @Test
  def retrieveDependentRowsSelfLink(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable("Table 1", 1)
      row11 <- addRow(table1, postDefaultTableRow("Row 1 in Table 1", 1))
      row12 <- addRow(table1, postDefaultTableRow("Row 2 in Table 1", 2))
      row13 <- addRow(table1, postDefaultTableRow("Row 3 in Table 1", 3))

      linkColumn1From1To1 <- createLinkColumn(table1, table1, singleDirection = true)

      _ <- putLink(table1, linkColumn1From1To1, row11, row12)
      _ <- putLink(table1, linkColumn1From1To1, row12, row13)
      _ <- putLink(table1, linkColumn1From1To1, row13, row11)

      result <- sendRequest("GET", s"/tables/$table1/rows/$row11/dependent")
      dependentRows11 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })

      result <- sendRequest("GET", s"/tables/$table1/rows/$row12/dependent")
      dependentRows12 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })

      result <- sendRequest("GET", s"/tables/$table1/rows/$row13/dependent")
      dependentRows13 = result
        .getJsonArray("dependentRows")
        .asScala
        .map({
          case o: JsonObject => o
        })
    } yield {
      val expectedDependentRows11 = Json.arr(
        Json.obj(
          "id" -> 2,
          "value" -> "Row 2 in Table 1"
        )
      )

      assertEquals(expectedDependentRows11, dependentRows11.head.getJsonArray("rows"))

      val expectedDependentRows12 = Json.arr(
        Json.obj(
          "id" -> 3,
          "value" -> "Row 3 in Table 1"
        )
      )

      assertEquals(expectedDependentRows12, dependentRows12.head.getJsonArray("rows"))

      val expectedDependentRows13 = Json.arr(
        Json.obj(
          "id" -> 1,
          "value" -> "Row 1 in Table 1"
        )
      )

      assertEquals(expectedDependentRows13, dependentRows13.head.getJsonArray("rows"))
    }
  }

  @Test
  def retrieveDependentRowsOfRowWithoutDependencies(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable("Table 1", 1)
      row11 <- addRow(table1)
      row12 <- addRow(table1)
      row13 <- addRow(table1)

      linkColumn1From1To1 <- createLinkColumn(table1, table1, singleDirection = true)

      result <- sendRequest("GET", s"/tables/$table1/rows/$row11/dependent")
      dependentRows11 = result.getJsonArray("dependentRows")
    } yield {
      val expectedDependentRows11 = Json.emptyArr()
      assertEquals(expectedDependentRows11, dependentRows11)
    }
  }

  @Test
  def retrieveDependentRowsOfTableWithoutLinks(implicit c: TestContext): Unit = okTest {
    for {
      table1 <- createEmptyDefaultTable("Table 1", 1)
      row11 <- addRow(table1)
      row12 <- addRow(table1)
      row13 <- addRow(table1)

      result <- sendRequest("GET", s"/tables/$table1/rows/$row11/dependent")
      dependentRows11 = result.getJsonArray("dependentRows")
    } yield {
      val expectedDependentRows11 = Json.emptyArr()
      assertEquals(expectedDependentRows11, dependentRows11)
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class LinkOrderTest extends LinkTestBase {

  @Test
  def testInsertionLinkOrder(implicit c: TestContext): Unit = {
    okTest{

      def putLink(toId: RowId) = Json.obj("value" -> Json.obj("to" -> toId))

      for {
        linkColumnId <- setupTwoTablesWithEmptyLinks()

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(1))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(3))

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(3))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(1))

        getFromTable1Row1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
        getFromTable1Row2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/2")

        getFromTable2Row1 <- sendRequest("GET", s"/tables/2/columns/$linkColumnId/rows/1")
        getFromTable2Row2 <- sendRequest("GET", s"/tables/2/columns/$linkColumnId/rows/2")
      } yield {

        import scala.collection.JavaConverters._

        assertEquals(List(1, 2, 3),
          getFromTable1Row1.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
        assertEquals(List(3, 2, 1),
          getFromTable1Row2.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))

        assertEquals(List(1, 2),
          getFromTable2Row1.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
        assertEquals(List(1, 2),
          getFromTable2Row2.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
      }
    }
  }

  @Test
  def testChangeOrderLocationEnd(implicit c: TestContext): Unit = {
    okTest{

      def putLink(toId: RowId) = Json.obj("value" -> Json.obj("to" -> toId))

      for {
        linkColumnId <- setupTwoTablesWithEmptyLinks()

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(1))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(3))

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(3))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(1))

        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/1/order", Json.obj("location" -> "end"))
        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/2/link/3/order", Json.obj("location" -> "end"))

        getFromTable1Row1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
        getFromTable1Row2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/2")
      } yield {
        import scala.collection.JavaConverters._

        assertEquals(List(2, 3, 1),
          getFromTable1Row1.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
        assertEquals(List(2, 1, 3),
          getFromTable1Row2.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
      }
    }
  }

  @Test
  def testChangeOrderLocationStart(implicit c: TestContext): Unit = {
    okTest{

      def putLink(toId: RowId) = Json.obj("value" -> Json.obj("to" -> toId))

      for {
        linkColumnId <- setupTwoTablesWithEmptyLinks()

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(1))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(3))

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(3))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(1))

        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/3/order", Json.obj("location" -> "start"))
        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/2/link/1/order", Json.obj("location" -> "start"))

        getFromTable1Row1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
        getFromTable1Row2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/2")
      } yield {
        import scala.collection.JavaConverters._

        assertEquals(List(3, 1, 2),
          getFromTable1Row1.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
        assertEquals(List(1, 3, 2),
          getFromTable1Row2.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
      }
    }
  }

  @Test
  def testChangeOrderLocationBefore(implicit c: TestContext): Unit = {
    okTest{

      def putLink(toId: RowId) = Json.obj("value" -> Json.obj("to" -> toId))

      for {
        linkColumnId <- setupTwoTablesWithEmptyLinks()

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(1))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(3))

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(3))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(2))
        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/2", putLink(1))

        _ <- sendRequest("PUT",
          s"/tables/1/columns/$linkColumnId/rows/1/link/2/order",
          Json.obj("location" -> "before", "id" -> 1))
        _ <- sendRequest("PUT",
          s"/tables/1/columns/$linkColumnId/rows/2/link/2/order",
          Json.obj("location" -> "before", "id" -> 3))

        getFromTable1Row1 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1")
        getFromTable1Row2 <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/2")
      } yield {
        import scala.collection.JavaConverters._

        assertEquals(List(2, 1, 3),
          getFromTable1Row1.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
        assertEquals(List(2, 3, 1),
          getFromTable1Row2.getJsonArray("value").asScala.map({ case obj: JsonObject => obj.getLong("id") }))
      }
    }
  }

  @Test
  def testWrongColumnType(implicit c: TestContext): Unit = exceptionTest("error.request.column.wrongtype") {
    for {
      _ <- setupTwoTables()
      _ <- sendRequest("PUT", s"/tables/1/columns/1/rows/1/link/1/order", Json.obj("location" -> "before", "id" -> 2))
    } yield ()
  }

  @Test
  def testWrongLocation(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {

    def putLink(toId: RowId) = Json.obj("value" -> Json.obj("to" -> toId))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(1))

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/1/order", Json.obj("location" -> "beef"))
    } yield ()
  }

  @Test
  def testWrongId(implicit c: TestContext): Unit = {
    exceptionTest("NOT FOUND"){

      def putLink(toId: RowId) = Json.obj("value" -> Json.obj("to" -> toId))

      for {
        linkColumnId <- setupTwoTablesWithEmptyLinks()

        _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", putLink(1))

        _ <- sendRequest("PUT",
          s"/tables/1/columns/$linkColumnId/rows/1/link/1/order",
          Json.obj("location" -> "before", "id" -> 2))
      } yield ()
    }
  }
}
