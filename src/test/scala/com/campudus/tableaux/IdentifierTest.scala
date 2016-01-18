package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json._

@RunWith(classOf[VertxUnitRunner])
class IdentifierTest extends TableauxTestBase {

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

    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "values" -> Json.arr(
        Json.arr("table1row1", 1, Json.emptyArr()),
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

      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveRowWithMultiLanguageIdentifierColumn(implicit c: TestContext): Unit = okTest {
    val linkColumn = Json.obj(
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

    val multilangTextColumn = Json.obj(
      "columns" -> Json.arr(
        Json.obj(
          "name" -> "Test Multilanguage",
          "kind" -> "text",
          "multilanguage" -> true
        )
      )
    )

    val expectedJson = Json.obj(
      "status" -> "ok",
      "id" -> 1,
      "values" -> Json.arr(
        Json.obj(
          "de-DE" -> Json.arr("table1row1", 1, "Hallo", Json.arr("Hallo")),
          "en-US" -> Json.arr("table1row1", 1, "Hello", Json.arr("Hello"))
        ),
        "table1row1",
        1,
        Json.obj(
          "de-DE" -> "Hallo",
          "en-US" -> "Hello"
        ),
        Json.arr(
          Json.obj(
            "id" -> 1,
            "value" -> Json.obj(
              "de-DE" -> "Hallo",
              "en-US" -> "Hello"
            )
          )
        )
      )
    )

    for {
      table1 <- setupDefaultTable()
      table2 <- setupDefaultTable("Test Table 2", 2)

      table2column3 <- sendRequest("POST", "/tables/2/columns", multilangTextColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      table1column3 <- sendRequest("POST", "/tables/1/columns", multilangTextColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      // create link column
      table1column4 <- sendRequest("POST", "/tables/1/columns", linkColumn) map {
        _.getArray("columns").get[JsonObject](0).getLong("id")
      }

      _ <- sendRequest("POST", "/tables/2/columns/3/rows/1", Json.obj("value" -> Json.obj("de-DE" -> "Hallo", "en-US" -> "Hello")))
      _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("de-DE" -> "Hallo", "en-US" -> "Hello")))

      _ <- sendRequest("PUT", "/tables/1/columns/4/rows/1", Json.obj("value" -> Json.obj("from" -> 1, "to" -> 1)))

      _ <- sendRequest("POST", "/tables/1/columns/1", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/2", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/3", Json.obj("identifier" -> true))
      _ <- sendRequest("POST", "/tables/1/columns/4", Json.obj("identifier" -> true))

      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
}
