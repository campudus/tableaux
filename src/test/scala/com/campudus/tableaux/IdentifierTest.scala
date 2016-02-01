package com.campudus.tableaux

import com.campudus.tableaux.testtools.RequestCreation._
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.{Ignore, Test}
import org.junit.runner.RunWith
import org.vertx.scala.core.json._

@RunWith(classOf[VertxUnitRunner])
class IdentifierTest extends TableauxTestBase {

  @Test
  def retrieveColumnsWithOneIdentifierColumn(implicit c: TestContext): Unit = okTest {
    for {
      _ <- setupDefaultTable()

      // make the last (ordering) column an identifier column
      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("identifier" -> true))

      test <- sendRequest("GET", "/tables/1/columns")
    } yield {
      // in case of one identifier column we don't get a concat column
      // but the identifier column will be the first
      assertEquals(2, test.getJsonArray("columns").get[JsonObject](0).getInteger("id"))
    }
  }

  @Test
  def retrieveColumnsWithTwoIdentifierColumn(implicit c: TestContext): Unit = okTest {
    val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 3")))

    for {
      _ <- setupDefaultTable()

      // create a third column
      _ <- sendRequest("POST", s"/tables/1/columns", createStringColumnJson)

      // make the first and the last an identifier column
      _ <- sendRequest("POST", "/tables/1/columns/1", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/3", Json.obj("identifier" -> true))

      test <- sendRequest("GET", "/tables/1/columns")
    } yield {
      // in case of two or more identifier columns we preserve the order of column
      // and a concatcolumn in front of all columns
      val concats = test.getJsonArray("columns").get[JsonObject](0).getJsonArray("concats")
      assertEquals(1L, concats.getJsonObject(0).getLong("id"))
      assertEquals(3L, concats.getJsonObject(1).getLong("id"))

      val columns = test.getJsonArray("columns")
      assertEquals(columns.getJsonObject(1), concats.getJsonObject(0))
      assertEquals(columns.getJsonObject(3), concats.getJsonObject(1))
    }
  }

  @Ignore("not implemented - like retrieveColumnsWithTwoIdentifierColumn, but re-order columns before doing this")
  @Test
  def retrieveColumnsWithTwoIdentifierColumnInCorrectOrder(implicit c: TestContext): Unit = okTest {
    // TODO Add test
    ???
  }

  @Ignore("not implemented - retrieve links on links on links ... that use concat columns")
  @Test
  def retrieveLinksOnConcatColumnsRecursively(implicit c: TestContext): Unit = okTest {
    // TODO Add test
    // create multiple tables that link on concat columns
    // verify that each concat has the needed column information
    // verify that the values are of the provided type
    // verify that the values are fetched recursively
    ???
  }

  @Test
  def retrieveLinkOnConcatColumn(implicit c: TestContext): Unit = okTest {
    def putLink(id: Long) = Json.obj("value" -> Json.obj("values" -> Json.arr(id)))
    def putLinks(ids: Seq[Long]) = Json.obj("value" -> Json.obj("values" -> Json.arr(ids: _*)))
    for {
      (tableId1, columnIds1, rowIds1) <- createSimpleTableWithValues("table1", List(Text("text11"), Identifier(Numeric("num12")), Identifier(Multilanguage(Text("multitext13"))), Numeric("num14")), List(
        List("table1col1row1", 121, Json.obj("de_DE" -> "table1col3row1-de", "en_GB" -> "table1col3row1-gb"), 141),
        List("table1col1row2", 122, Json.obj("de_DE" -> "table1col3row2-de", "en_GB" -> "table1col3row2-gb"), 142)
      ))
      (tableId2, columnIds2, rowIds2) <- createSimpleTableWithValues("table2", List(Text("text21"), Identifier(Numeric("num22")), Identifier(Multilanguage(Text("multitext23"))), Numeric("num24")), List(
        List("table2col1row1", 221, Json.obj("de_DE" -> "table2col3row1-de", "en_GB" -> "table2col3row1-gb"), 241),
        List("table2col1row2", 222, Json.obj("de_DE" -> "table2col3row2-de", "en_GB" -> "table2col3row2-gb"), 242)
      ))
      (tableId3, columnIds3, rowIds3) <- createSimpleTableWithValues("table2", List(Text("text31"), Identifier(Numeric("num32")), Identifier(Multilanguage(Text("multitext23"))), Numeric("num34")), List(
        List("table3col1row1", 321, Json.obj("de_DE" -> "table3col3row1-de", "en_GB" -> "table3col3row1-gb"), 341),
        List("table3col1row2", 322, Json.obj("de_DE" -> "table3col3row2-de", "en_GB" -> "table3col3row2-gb"), 342)
      ))

      // add link column from table 1 to table 2 and make it identifier
      postLinkColTable2 = Json.obj("columns" -> Json.arr(Json.obj("name" -> "link251", "kind" -> "link", "fromColumn" -> 1, "toTable" -> tableId1, "toColumn" -> 1, "identifier" -> true)))
      linkColRes2 <- sendRequest("POST", s"/tables/$tableId2/columns", postLinkColTable2)
      linkColId2 = linkColRes2.getJsonArray("columns").getJsonObject(0).getLong("id")
      postLinkColTable3 = Json.obj("columns" -> Json.arr(Json.obj("name" -> "link352", "kind" -> "link", "fromColumn" -> 1, "toTable" -> tableId2, "toColumn" -> 1)))
      linkColRes3 <- sendRequest("POST", s"/tables/$tableId3/columns", postLinkColTable3)
      linkColId3 = linkColRes3.getJsonArray("columns").getJsonObject(0).getLong("id")

      // add link from table2row1 to table1row1 and to table1row2
      _ <- sendRequest("POST", s"/tables/$tableId2/columns/$linkColId2/rows/${rowIds2.head}", putLinks(rowIds1))

      // add link from table2row2 to table1row1
      _ <- sendRequest("POST", s"/tables/$tableId2/columns/$linkColId2/rows/${rowIds2(1)}", putLink(rowIds1.head))

      // add link from table3row1 to table2row1 and to table2row2
      _ <- sendRequest("POST", s"/tables/$tableId3/columns/$linkColId3/rows/${rowIds3.head}", putLinks(rowIds2))

      columnResult <- sendRequest("GET", s"/tables/$tableId3/columns/$linkColId3")
      cellResult <- sendRequest("GET", s"/tables/$tableId3/columns/$linkColId3/rows/${rowIds3.head}")
    } yield {

      val toColumn = columnResult.getJsonObject("toColumn")
      assertEquals("concat", toColumn.getString("kind"))
      val concats = toColumn.getJsonArray("concats")
      assertEquals(3, concats.size)
      val concats1 = concats.getJsonObject(0)
      val concats2 = concats.getJsonObject(1)
      val concats3 = concats.getJsonObject(2)
      assertEquals(columnIds2(1), concats1.getLong("id"))
      assertEquals("numeric", concats1.getString("kind"))
      assertEquals(columnIds2(2), concats2.getLong("id"))
      assertEquals("text", concats2.getString("kind"))
      assertEquals(true, concats2.getBoolean("multilanguage"))
      assertEquals(linkColId2, concats3.getLong("id"))
      assertEquals("link", concats3.getString("kind"))
    }
  }

  @Test
  def retrieveConcatColumn(implicit c: TestContext): Unit = okTest {
    val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 3")))

    for {
      _ <- setupDefaultTable()

      // create a third column
      _ <- sendRequest("POST", s"/tables/1/columns", createStringColumnJson)

      // make the first and the last an identifier column
      _ <- sendRequest("POST", "/tables/1/columns/1", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/3", Json.obj("identifier" -> true))

      test <- sendRequest("GET", "/tables/1/columns/0")
    } yield {
      val first = test.getJsonArray("concats").getJsonObject(0)
      val second = test.getJsonArray("concats").getJsonObject(1)
      assertEquals(1L, first.getLong("id"))
      assertEquals(3L, second.getLong("id"))
      assertEquals("concat", test.getString("kind"))
    }
  }

  @Test
  def retrieveRowWithSingleLanguageIdentifierColumn(implicit c: TestContext): Unit = okTest {
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

    val expectedCellValue = Json.arr("table1row1", 1, Json.emptyArr())
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "values" -> Json.arr(
        expectedCellValue,
        "table1row1",
        1,
        Json.emptyArr()
      )
    )

    for {
      _ <- setupDefaultTable()
      _ <- setupDefaultTable("Test Table 2", 2)

      // create link column
      linkColumnId <- sendRequest("POST", "/tables/1/columns", linkColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      _ <- sendRequest("POST", "/tables/1/columns/1", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/3", Json.obj("identifier" -> true))

      testRow <- sendRequest("GET", "/tables/1/rows/1")
      testCell <- sendRequest("GET", "/tables/1/columns/0/rows/1")
    } yield {
      assertEquals(expectedJson, testRow)
      assertEquals(expectedCellValue, testCell.getJsonArray("value"))
    }
  }

  @Test
  def retrieveRowWithMultiLanguageIdentifierColumn(implicit c: TestContext): Unit = okTest {
    val linkColumnToTable2 = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "fromColumn" -> 1,
          "toTable" -> 2,
          "toColumn" -> 3
        )
      )
    )

    val linkColumnToTable3 = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Link 1",
          "kind" -> "link",
          "fromColumn" -> 1,
          "toTable" -> 3,
          "toColumn" -> 1
        )
      )
    )

    val multilangTextColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Multilanguage",
          "kind" -> "text",
          "multilanguage" -> true
        )
      )
    )

    val expectedCellValue = Json.arr(
      "table1row1",
      1,
      Json.obj(
        "de-DE" -> "Tschüss",
        "en-US" -> "Goodbye"
      )
    )
    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "values" -> Json.arr(
        expectedCellValue,
        "table1row1",
        1,
        Json.obj(
          "de-DE" -> "Tschüss",
          "en-US" -> "Goodbye"
        ),
        Json.arr(
          Json.obj(
            "id" -> 1,
            "value" -> Json.arr(
              "table2row1",
              Json.arr(
                Json.obj(
                  "id" -> 1,
                  "value" -> "table3row1"
                )
              )
            )
          )
        )
      )
    )

    for {
      table1 <- setupDefaultTable()
      table2 <- setupDefaultTable("Test Table 2", 2)
      table3 <- setupDefaultTable("Test Table 3", 3)

      // create multi-language column and fill cell
      table1column3 <- sendRequest("POST", "/tables/1/columns", multilangTextColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }
      _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("de-DE" -> "Tschüss", "en-US" -> "Goodbye")))

      // create multi-language column and fill cell
      table2column3 <- sendRequest("POST", "/tables/2/columns", multilangTextColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }
      _ <- sendRequest("POST", "/tables/2/columns/3/rows/1", Json.obj("value" -> Json.obj("de-DE" -> "Hallo", "en-US" -> "Hello")))

      // create link column, which will link to concatcolumn in this case
      table1column4 <- sendRequest("POST", "/tables/1/columns", linkColumnToTable2) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }
      _ <- sendRequest("PUT", "/tables/1/columns/4/rows/1", Json.obj("value" -> Json.obj("from" -> 1, "to" -> 1)))

      // create link column, which will link to concatcolumn in this case
      table2column5 <- sendRequest("POST", "/tables/2/columns", linkColumnToTable3) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }
      _ <- sendRequest("PUT", "/tables/2/columns/5/rows/1", Json.obj("value" -> Json.obj("from" -> 1, "to" -> 1)))

      _ <- sendRequest("POST", "/tables/1/columns/1", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/3", Json.obj("identifier" -> true))

      _ <- sendRequest("POST", "/tables/2/columns/1", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/2/columns/5", Json.obj("identifier" -> true))

      testRow <- sendRequest("GET", "/tables/1/rows/1")
      testCell <- sendRequest("GET", "/tables/1/columns/0/rows/1")
    } yield {
      assertEquals(expectedJson, testRow)
      assertEquals(expectedCellValue, testCell.getJsonArray("value"))
    }
  }
}
