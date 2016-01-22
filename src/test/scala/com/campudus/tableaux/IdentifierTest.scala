package com.campudus.tableaux

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
      assertEquals(Json.arr(1, 3), test.getJsonArray("columns").get[JsonObject](0).getJsonArray("concats"))

      assertEquals(1, test.getJsonArray("columns").get[JsonObject](1).getInteger("id"))
      assertEquals(3, test.getJsonArray("columns").get[JsonObject](3).getInteger("id"))
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
      assertEquals(Json.arr(1, 3), test.getJsonArray("concats"))
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
