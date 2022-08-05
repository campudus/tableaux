package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.{TableauxTestBase, TestCustomException}
import com.campudus.tableaux.testtools.RequestCreation._

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class ColumnValuesTest extends TableauxTestBase {

  @Test
  def retrieveColumnValuesFromLanguageNeutralColumn(implicit c: TestContext): Unit = okTest {
    val createStringColumnJson =
      Json.obj("columns" -> Json.arr(Json.obj("kind" -> "shorttext", "name" -> "column", "identifier" -> true)))

    val expectedJson = Json.obj(
      "status" -> "ok",
      "values" -> Json.arr(
        "a",
        "b",
        "c"
      )
    )

    for {
      // prepare table
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "test")).map(_.getLong("id"))
      columns <- sendRequest("POST", s"/tables/$tableId/columns", createStringColumnJson).map(_.getJsonArray("columns"))

      // add rows
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> "a")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> "c")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> "b")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> "a")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> "b")))

      // add some empty values
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> " ")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> " ")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> "")))
      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      test <- sendRequest("GET", s"/tables/$tableId/columns/1/values")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def retrieveColumnValuesFromMultiLanguageColumn(implicit c: TestContext): Unit = okTest {
    val createStringColumnJson =
      Json.obj(
        "columns" -> Json.arr(
          Json.obj("kind" -> "shorttext", "name" -> "column", "identifier" -> true, "languageType" -> "language")
        )
      )

    val expectedJson1 = Json.obj(
      "status" -> "ok",
      "values" -> Json.arr(
        "a",
        "b",
        "c"
      )
    )

    val expectedJson2 = Json.obj(
      "status" -> "ok",
      "values" -> Json.arr(
        "a",
        "d"
      )
    )

    for {
      // prepare table
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "test")).map(_.getLong("id"))
      columns <- sendRequest("POST", s"/tables/$tableId/columns", createStringColumnJson).map(_.getJsonArray("columns"))

      // add rows
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> "a"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("en" -> "a"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> "c"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> "b"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> "a"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> "b"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("en" -> "d"))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("en" -> "a"))))

      // add some empty values
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> " "))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> " "))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("de" -> ""))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows", Rows(columns, Json.obj("column" -> Json.obj("en" -> ""))))
      _ <- sendRequest("POST", s"/tables/$tableId/rows")

      test1 <- sendRequest("GET", s"/tables/$tableId/columns/1/values/de")
      test2 <- sendRequest("GET", s"/tables/$tableId/columns/1/values/en")
    } yield {
      assertEquals(expectedJson1, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def retrieveColumnValuesFromUnsupportedColumns(implicit c: TestContext): Unit = okTest {
    val createShortTextCountryColumnJson = MultiCountry(ShortTextCol("column1"), Seq("DE"))
    val createShortTextColumnJson = ShortTextCol("column2")
    val createNumericColumnJson = NumericCol("column3")
    val createBooleanColumnJson = BooleanCol("column4")

    for {
      // prepare table
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "test")).map(_.getLong("id"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns", Columns(createShortTextCountryColumnJson))
        .map(_.getJsonArray("columns"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns", Columns(createShortTextColumnJson))
        .map(_.getJsonArray("columns"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns", Columns(createNumericColumnJson))
        .map(_.getJsonArray("columns"))
      _ <- sendRequest("POST", s"/tables/$tableId/columns", Columns(createBooleanColumnJson))
        .map(_.getJsonArray("columns"))

      _ <- sendRequest("GET", s"/tables/$tableId/columns/1/values/de")
        .flatMap(_ => Future.failed(new Exception("request should fail b/c multi-country is not allowed")))
        .recoverWith({
          case TestCustomException(_, _, 422) => Future.successful(())
        })
      _ <- sendRequest("GET", s"/tables/$tableId/columns/2/values/de")
        .flatMap(_ => Future.failed(new Exception("request should fail b/c shouldn't be called with langtag")))
        .recoverWith({
          case TestCustomException(_, _, 422) => Future.successful(())
        })
      _ <- sendRequest("GET", s"/tables/$tableId/columns/3/values")
        .flatMap(_ => Future.failed(new Exception("request should fail b/c numeric is not allowed")))
        .recoverWith({
          case TestCustomException(_, _, 400) => Future.successful(())
        })
      _ <- sendRequest("GET", s"/tables/$tableId/columns/4/values")
        .flatMap(_ => Future.failed(new Exception("request should fail b/c boolean is not allowed")))
        .recoverWith({
          case TestCustomException(_, _, 400) => Future.successful(())
        })
    } yield ()
  }
}
