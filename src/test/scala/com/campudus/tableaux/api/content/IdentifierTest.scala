package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json._

@RunWith(classOf[VertxUnitRunner])
class IdentifierTest extends TableauxTestBase {

  @Test
  def retrieveColumnsWithOneIdentifierColumn(implicit c: TestContext): Unit = okTest {
    for {
      _ <- createDefaultTable()

      // make the last (ordering) column an identifier column
      _ <- sendRequest("POST", "/tables/1/columns/1", Json.obj("identifier" -> false))
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
      _ <- createDefaultTable()

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
      // actually we have 4 columns because of the ConcatColumn
      assertEquals(columns.getJsonObject(1), concats.getJsonObject(0))
      assertEquals(columns.getJsonObject(3), concats.getJsonObject(1))
    }
  }

  @Test
  def retrieveColumnsWithTwoIdentifierColumnInCorrectOrder(implicit c: TestContext): Unit = {
    okTest {
      val createStringColumnJson = Json
        .obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 3")))

      for {
        _ <- createDefaultTable()

        // create a third column
        _ <- sendRequest("POST", s"/tables/1/columns", createStringColumnJson)

        // make the first and the last an identifier column and reorder them
        _ <- sendRequest(
          "POST",
          "/tables/1/columns/1",
          Json.obj("name" -> "Column 1 but second concat column", "identifier" -> true, "ordering" -> 3))
        _ <- sendRequest("POST",
                         "/tables/1/columns/3",
                         Json.obj("name" -> "Column 3 but first concat column", "identifier" -> true, "ordering" -> 1))

        testColumns <- sendRequest("GET", "/tables/1/columns")

        _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", Json.obj("value" -> "table 1 column 3 row 1"))
        _ <- sendRequest("POST", "/tables/1/columns/3/rows/2", Json.obj("value" -> "table 1 column 3 row 2"))

        testCells <- sendRequest("GET", "/tables/1/rows")
      } yield {
        // in case of two or more identifier columns we preserve the order of column
        // and a concatcolumn in front of all columns
        val concats = testColumns.getJsonArray("columns").get[JsonObject](0).getJsonArray("concats")
        assertEquals(3L, concats.getJsonObject(0).getLong("id"))
        assertEquals(1L, concats.getJsonObject(1).getLong("id"))

        // actually we have 4 columns because of the ConcatColumn
        // (0) concat, (1) column 3, (2) columnn 2, (3) column 1
        assertEquals("Column 3 but first concat column", concats.getJsonObject(0).getString("name"))
        assertEquals("Column 1 but second concat column", concats.getJsonObject(1).getString("name"))

        val excepted = Json.arr(
          Json.obj(
            "id" -> 1,
            "values" -> Json.arr(
              Json.arr("table 1 column 3 row 1", "table1row1"),
              "table 1 column 3 row 1",
              1,
              "table1row1"
            )
          ),
          Json.obj(
            "id" -> 2,
            "values" -> Json.arr(
              Json.arr("table 1 column 3 row 2", "table1row2"),
              "table 1 column 3 row 2",
              2,
              "table1row2"
            )
          )
        )

        assertEquals(excepted, testCells.getJsonArray("rows"))
      }
    }
  }

  @Test
  def retrieveLinksOnConcatColumnsRecursively(implicit c: TestContext): Unit = {
    okTest {

      def putLink(id: Long) = Json.obj("value" -> Json.obj("values" -> Json.arr(id)))

      def putLinks(ids: Seq[Long]) = Json.obj("value" -> Json.obj("values" -> Json.arr(ids: _*)))
      for {

        // create multiple tables
        (tableId1, columnIds1, rowIds1) <- createSimpleTableWithValues(
          "table1",
          List(TextCol("text11"),
               Identifier(NumericCol("num12")),
               Identifier(Multilanguage(TextCol("multitext13"))),
               NumericCol("num14")),
          List(
            List("table1col1row1", 121, Json.obj("de_DE" -> "table1col3row1-de", "en_GB" -> "table1col3row1-gb"), 141),
            List("table1col1row2", 122, Json.obj("de_DE" -> "table1col3row2-de", "en_GB" -> "table1col3row2-gb"), 142)
          )
        )
        (tableId2, columnIds2, rowIds2) <- createSimpleTableWithValues(
          "table2",
          List(TextCol("text21"),
               Identifier(NumericCol("num22")),
               Identifier(Multilanguage(TextCol("multitext23"))),
               NumericCol("num24")),
          List(
            List("table2col1row1", 221, Json.obj("de_DE" -> "table2col3row1-de", "en_GB" -> "table2col3row1-gb"), 241),
            List("table2col1row2", 222, Json.obj("de_DE" -> "table2col3row2-de", "en_GB" -> "table2col3row2-gb"), 242)
          )
        )
        (tableId3, columnIds3, rowIds3) <- createSimpleTableWithValues(
          "table3",
          List(TextCol("text31"),
               Identifier(NumericCol("num32")),
               Identifier(Multilanguage(TextCol("multitext33"))),
               NumericCol("num34")),
          List(
            List("table3col1row1", 321, Json.obj("de_DE" -> "table3col3row1-de", "en_GB" -> "table3col3row1-gb"), 341),
            List("table3col1row2", 322, Json.obj("de_DE" -> "table3col3row2-de", "en_GB" -> "table3col3row2-gb"), 342)
          )
        )
        (tableId4, columnIds4, rowIds4) <- createSimpleTableWithValues(
          "table4",
          List(TextCol("text41"),
               Identifier(NumericCol("num42")),
               Identifier(Multilanguage(TextCol("multitext43"))),
               NumericCol("num44")),
          List(
            List("table4col1row1", 421, Json.obj("de_DE" -> "table4col3row1-de", "en_GB" -> "table4col3row1-gb"), 441),
            List("table4col1row2", 422, Json.obj("de_DE" -> "table4col3row2-de", "en_GB" -> "table4col3row2-gb"), 442)
          )
        )

        // link from table 2 to table 1
        postLinkColTable2 = Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "link251",
                     "kind" -> "link",
                     "toTable" -> tableId1,
                     "identifier" -> true,
                     "singleDirection" -> true)))
        linkColRes2 <- sendRequest("POST", s"/tables/$tableId2/columns", postLinkColTable2)
        linkColId2 = linkColRes2.getJsonArray("columns").getJsonObject(0).getLong("id")

        // link from table 3 to table 2
        postLinkColTable3 = Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "link352",
                     "kind" -> "link",
                     "toTable" -> tableId2,
                     "identifier" -> true,
                     "singleDirection" -> true)))
        linkColRes3 <- sendRequest("POST", s"/tables/$tableId3/columns", postLinkColTable3)
        linkColId3 = linkColRes3.getJsonArray("columns").getJsonObject(0).getLong("id")

        // link from table 4 to table 3
        postLinkColTable4 = Json.obj(
          "columns" -> Json.arr(Json.obj("name" -> "link452", "kind" -> "link", "toTable" -> tableId3)))
        linkColRes4 <- sendRequest("POST", s"/tables/$tableId4/columns", postLinkColTable4)
        linkColId4 = linkColRes4.getJsonArray("columns").getJsonObject(0).getLong("id")

        // verify that each concat has the needed column information
        // verify that the values are of the provided type
        // verify that the values are fetched recursively

        // add link from table2row1 to table1row1 and to table1row2
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/$linkColId2/rows/${rowIds2.head}", putLinks(rowIds1))

        // add link from table2row2 to table1row1
        _ <- sendRequest("POST", s"/tables/$tableId2/columns/$linkColId2/rows/${rowIds2(1)}", putLink(rowIds1.head))

        // add link from table3row1 to table2row1 and to table2row2
        _ <- sendRequest("POST", s"/tables/$tableId3/columns/$linkColId3/rows/${rowIds3.head}", putLinks(rowIds2))

        // add link from table4row1 to table3row1
        _ <- sendRequest("POST", s"/tables/$tableId4/columns/$linkColId4/rows/${rowIds4.head}", putLink(rowIds3.head))

        columnResult <- sendRequest("GET", s"/tables/$tableId4/columns/$linkColId4")
        cellResult <- sendRequest("GET", s"/tables/$tableId4/columns/$linkColId4/rows/${rowIds4.head}")
      } yield {

        // check columns
        logger.info(s"columnResult=${columnResult.encode()}")
        // concat columns to table 3
        val toColumn4 = columnResult.getJsonObject("toColumn")
        assertEquals("concat", toColumn4.getString("kind"))
        val concatsTo3 = toColumn4.getJsonArray("concats")
        assertEquals(3, concatsTo3.size)

        val concatColumn31 = concatsTo3.getJsonObject(0)
        val concatColumn32 = concatsTo3.getJsonObject(1)
        val concatColumn33 = concatsTo3.getJsonObject(2)
        assertEquals(columnIds3(1), concatColumn31.getLong("id"))
        assertEquals("numeric", concatColumn31.getString("kind"))
        assertEquals(columnIds3(2), concatColumn32.getLong("id"))
        assertEquals("text", concatColumn32.getString("kind"))
        assertEquals(true, concatColumn32.getBoolean("multilanguage"))
        assertEquals(linkColId3, concatColumn33.getLong("id"))
        assertEquals("link", concatColumn33.getString("kind"))

        // concat columns to table 2
        assertEquals(tableId2, concatColumn33.getLong("toTable"))
        val toTable2 = concatColumn33.getJsonObject("toColumn")
        assertEquals(0.toLong, toTable2.getLong("id"))
        assertEquals("concat", toTable2.getString("kind"))
        val concatsTo2 = toTable2.getJsonArray("concats")
        assertEquals(3, concatsTo2.size())

        val concatColumn21 = concatsTo2.getJsonObject(0)
        val concatColumn22 = concatsTo2.getJsonObject(1)
        val concatColumn23 = concatsTo2.getJsonObject(2)
        assertEquals(columnIds2(1), concatColumn21.getLong("id"))
        assertEquals("numeric", concatColumn21.getString("kind"))
        assertEquals(columnIds2(2), concatColumn22.getLong("id"))
        assertEquals("text", concatColumn22.getString("kind"))
        assertEquals(true, concatColumn22.getBoolean("multilanguage"))
        assertEquals(linkColId2, concatColumn23.getLong("id"))
        assertEquals("link", concatColumn23.getString("kind"))

        // concat columns to table 1
        assertEquals(tableId1, concatColumn23.getLong("toTable"))
        val toTable1 = concatColumn23.getJsonObject("toColumn")
        assertEquals(0.toLong, toTable1.getLong("id"))
        assertEquals("concat", toTable1.getString("kind"))
        val concatsTo1 = toTable1.getJsonArray("concats")
        assertEquals(2, concatsTo1.size())

        val concatColumn11 = concatsTo2.getJsonObject(0)
        val concatColumn12 = concatsTo2.getJsonObject(1)
        assertEquals(columnIds1(1), concatColumn11.getLong("id"))
        assertEquals("numeric", concatColumn11.getString("kind"))
        assertEquals(columnIds1(2), concatColumn12.getLong("id"))
        assertEquals("text", concatColumn12.getString("kind"))
        assertEquals(true, concatColumn12.getBoolean("multilanguage"))

        // check cell results
        logger.info(s"cellResult=${cellResult.encode()}")
        assertEquals(
          Json.obj(
            "value" -> Json.arr(
              // single link into table 3 row 1
              Json.obj(
                "id" -> rowIds3.head,
                "value" -> Json.arr(
                  321,
                  Json.obj("de_DE" -> "table3col3row1-de", "en_GB" -> "table3col3row1-gb"),
                  // links into table 2 row 1 and row 2
                  Json.arr(
                    Json.obj(
                      "id" -> rowIds2.head,
                      "value" -> Json.arr(
                        221,
                        Json.obj("de_DE" -> "table2col3row1-de", "en_GB" -> "table2col3row1-gb"),
                        // links into table 1 row 1 and row 2
                        Json.arr(
                          Json.obj("id" -> rowIds1.head,
                                   "value" -> Json.arr(
                                     121,
                                     Json.obj("de_DE" -> "table1col3row1-de", "en_GB" -> "table1col3row1-gb")
                                   )),
                          Json.obj("id" -> rowIds1(1),
                                   "value" -> Json.arr(
                                     122,
                                     Json.obj("de_DE" -> "table1col3row2-de", "en_GB" -> "table1col3row2-gb")
                                   ))
                        )
                      )
                    ),
                    Json.obj(
                      "id" -> rowIds2(1),
                      "value" -> Json.arr(
                        222,
                        Json.obj("de_DE" -> "table2col3row2-de", "en_GB" -> "table2col3row2-gb"),
                        // links into table 1 row 1
                        Json.arr(
                          Json.obj("id" -> rowIds1.head,
                                   "value" -> Json.arr(
                                     121,
                                     Json.obj("de_DE" -> "table1col3row1-de", "en_GB" -> "table1col3row1-gb")
                                   ))
                        )
                      )
                    )
                  )
                )
              )
            ),
            "status" -> "ok"
          ),
          cellResult
        )
      }
    }
  }

  @Test
  def retrieveLinkOnConcatColumn(implicit c: TestContext): Unit = {
    okTest {

      def putLink(id: Long) = Json.obj("value" -> Json.obj("values" -> Json.arr(id)))

      def putLinks(ids: Seq[Long]) = Json.obj("value" -> Json.obj("values" -> Json.arr(ids: _*)))
      for {
        (tableId1, columnIds1, rowIds1) <- createSimpleTableWithValues(
          "table1",
          List(TextCol("text11"),
               Identifier(NumericCol("num12")),
               Identifier(Multilanguage(TextCol("multitext13"))),
               NumericCol("num14")),
          List(
            List("table1col1row1", 121, Json.obj("de_DE" -> "table1col3row1-de", "en_GB" -> "table1col3row1-gb"), 141),
            List("table1col1row2", 122, Json.obj("de_DE" -> "table1col3row2-de", "en_GB" -> "table1col3row2-gb"), 142)
          )
        )
        (tableId2, columnIds2, rowIds2) <- createSimpleTableWithValues(
          "table2",
          List(TextCol("text21"),
               Identifier(NumericCol("num22")),
               Identifier(Multilanguage(TextCol("multitext23"))),
               NumericCol("num24")),
          List(
            List("table2col1row1", 221, Json.obj("de_DE" -> "table2col3row1-de", "en_GB" -> "table2col3row1-gb"), 241),
            List("table2col1row2", 222, Json.obj("de_DE" -> "table2col3row2-de", "en_GB" -> "table2col3row2-gb"), 242)
          )
        )
        (tableId3, columnIds3, rowIds3) <- createSimpleTableWithValues(
          "table3",
          List(TextCol("text31"),
               Identifier(NumericCol("num32")),
               Identifier(Multilanguage(TextCol("multitext33"))),
               NumericCol("num34")),
          List(
            List("table3col1row1", 321, Json.obj("de_DE" -> "table3col3row1-de", "en_GB" -> "table3col3row1-gb"), 341),
            List("table3col1row2", 322, Json.obj("de_DE" -> "table3col3row2-de", "en_GB" -> "table3col3row2-gb"), 342)
          )
        )

        // add link column from table 2 and make it identifier
        postLinkColTable2 = Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "link251",
                     "kind" -> "link",
                     "toTable" -> tableId1,
                     "identifier" -> true,
                     "singleDirection" -> true)))
        linkColRes2 <- sendRequest("POST", s"/tables/$tableId2/columns", postLinkColTable2)
        linkColId2 = linkColRes2.getJsonArray("columns").getJsonObject(0).getLong("id")
        // add link from table 3 to table 2 (with 3 identifiers)
        postLinkColTable3 = Json.obj(
          "columns" -> Json.arr(Json.obj("name" -> "link352", "kind" -> "link", "toTable" -> tableId2)))
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
  }

  @Test
  def retrieveConcatColumn(implicit c: TestContext): Unit = okTest {
    val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 3")))

    for {
      _ <- createDefaultTable()

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
          "toTable" -> 2
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
      _ <- createDefaultTable()
      _ <- createDefaultTable("Test Table 2", 2)

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
  def retrieveRowWithMultiLanguageIdentifierColumn(implicit c: TestContext): Unit = {
    okTest {
      val linkColumnToTable2 = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "name" -> "Test Link 1",
            "kind" -> "link",
            "toTable" -> 2
          )
        )
      )

      val linkColumnToTable3 = Json.obj(
        "columns" -> Json.arr(
          Json.obj(
            "name" -> "Test Link 1",
            "kind" -> "link",
            "toTable" -> 3
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
        table1 <- createDefaultTable()
        table2 <- createDefaultTable("Test Table 2", 2)
        table3 <- createDefaultTable("Test Table 3", 3)

        // create multi-language column and fill cell
        table1column3 <- sendRequest("POST", "/tables/1/columns", multilangTextColumn) map {
          _.getArray("columns").get[JsonObject](0).getLong("id")
        }
        _ <- sendRequest("POST",
                         "/tables/1/columns/3/rows/1",
                         Json.obj("value" -> Json.obj("de-DE" -> "Tschüss", "en-US" -> "Goodbye")))

        // create multi-language column and fill cell
        table2column3 <- sendRequest("POST", "/tables/2/columns", multilangTextColumn) map {
          _.getArray("columns").get[JsonObject](0).getLong("id")
        }
        _ <- sendRequest("POST",
                         "/tables/2/columns/3/rows/1",
                         Json.obj("value" -> Json.obj("de-DE" -> "Hallo", "en-US" -> "Hello")))

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
}
