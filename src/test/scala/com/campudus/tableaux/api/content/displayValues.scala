package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject, JsonArray}

@RunWith(classOf[VertxUnitRunner])
class DisplayValueTest extends LinkTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  val createNumberColumnJson =
    Json.obj(
      "columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column Number", "multilanguage" -> true)))

  val createCurrencyColumnJson =
    Json.obj(
      "columns" -> Json.arr(Json.obj("kind" -> "currency", "name" -> "Test Column Currency", "multilanguage" -> true)))

  val createBooleanColumnJson =
    Json.obj(
      "columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column Bool", "multilanguage" -> true)))

  val createDateColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "date", "name" -> "Test Column Date", "multilanguage" -> true)))

  val createDateTimeColumnJson =
    Json.obj(
      "columns" -> Json.arr(Json.obj("kind" -> "datetime", "name" -> "Test Column DateTime", "multilanguage" -> true)))

  val createTextColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column Text", "multilanguage" -> true)))

  val createValuePost = (value: Any) => {
    Json.obj("value" -> value)
  }
  val putNumberValue = createValuePost(Json.obj("de-DE" -> 42.42))
  val putCurrencyValue = createValuePost(Json.obj("DE" -> 1000.42))
  val putBooleanValue = createValuePost(Json.obj("de-DE" -> true))
  val putDateValue = createValuePost(Json.obj("de-DE" -> "2015-01-01"))
  val putDateTimeValue = createValuePost(Json.obj("de-DE" -> "2015-01-01T13:37:47.110Z"))
  val putTextValue = createValuePost(Json.obj("de-DE" -> "this is text"))

  val expectedNumber = Json.obj(
    "de-DE" -> "42.42",
    "en-GB" -> ""
  )

  val expectedCurrency = Json.obj(
    "de-DE" -> "1000,42 EUR",
    "en-GB" -> ""
  )

  val expectedBool = Json.obj(
    "de-DE" -> "Test Column Bool",
    "en-GB" -> ""
  )

  val expectedDate = Json.obj(
    "de-DE" -> "01.01.2015",
    "en-GB" -> ""
  )

  val expectedDateTime = Json.obj(
    "de-DE" -> "01.01.2015 - 01:37",
    "en-GB" -> ""
  )

  val expectedText = Json.obj(
    "de-DE" -> "this is text",
    "en-GB" -> ""
  )

  val expectedLink = Json.arr(
    Json.obj(
      "de-DE" -> "table2RowId1",
      "en-GB" -> "table2RowId1"
    ))

  def getLinksValue(arr: JsonArray, pos: Int): JsonArray = {
    arr.getJsonObject(pos).getJsonArray("value")
  }

  def toRowsArray(obj: JsonObject): JsonArray = {
    obj.getJsonArray("rows")
  }

  def getDisplayValue(obj: JsonObject): JsonObject = {
    obj.getJsonObject("displayValue")
  }

  def getDisplayValueArray(obj: JsonObject): JsonArray = {
    obj.getJsonArray("displayValue")
  }

  def getColumnId(obj: JsonObject): String = {
    obj.getJsonArray("columns").getJsonObject(0).getInteger("id").toString
  }

  @Test
  def retrieveRowWithDisplayValues(implicit c: TestContext): Unit = okTest {

    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(3)))

    val expected =
      """
        |[
        |  {"id": 3, "value": "table2RowId1"}
        |]
        |""".stripMargin

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putLink)
      textCol <- sendRequest("POST", s"/tables/1/columns", createTextColumnJson)
      currencyCol <- sendRequest("POST", s"/tables/1/columns", createCurrencyColumnJson)
      boolCol <- sendRequest("POST", s"/tables/1/columns", createBooleanColumnJson)
      dateCol <- sendRequest("POST", s"/tables/1/columns", createDateColumnJson)
      dateTimeCol <- sendRequest("POST", s"/tables/1/columns", createDateTimeColumnJson)
      numberCol <- sendRequest("POST", s"/tables/1/columns", createNumberColumnJson)

      _ <- sendRequest("POST", s"/tables/1/columns/${getColumnId(textCol)}/rows/1", putTextValue)
      _ <- sendRequest("POST", s"/tables/1/columns/${getColumnId(currencyCol)}/rows/1", putCurrencyValue)
      _ <- sendRequest("POST", s"/tables/1/columns/${getColumnId(boolCol)}/rows/1", putBooleanValue)
      _ <- sendRequest("POST", s"/tables/1/columns/${getColumnId(dateCol)}/rows/1", putDateValue)
      _ <- sendRequest("POST", s"/tables/1/columns/${getColumnId(dateTimeCol)}/rows/1", putDateTimeValue)
      _ <- sendRequest("POST", s"/tables/1/columns/${getColumnId(numberCol)}/rows/1", putNumberValue)

      linkRes <- sendRequest("GET", s"/tables/1/columns/${linkColumnId}/rows/1")
      textRes <- sendRequest("GET", s"/tables/1/columns/${getColumnId(textCol)}/rows/1")
      currencyRes <- sendRequest("GET", s"/tables/1/columns/${getColumnId(currencyCol)}/rows/1")
      boolRes <- sendRequest("GET", s"/tables/1/columns/${getColumnId(boolCol)}/rows/1")
      dateRes <- sendRequest("GET", s"/tables/1/columns/${getColumnId(dateCol)}/rows/1")
      dateTimeRes <- sendRequest("GET", s"/tables/1/columns/${getColumnId(dateTimeCol)}/rows/1")
      numberRes <- sendRequest("GET", s"/tables/1/columns/${getColumnId(numberCol)}/rows/1")

    } yield {
      assertJSONEquals(expectedText, getDisplayValue(textRes))
      assertJSONEquals(expectedCurrency, getDisplayValue(currencyRes))
      assertJSONEquals(expectedBool, getDisplayValue(boolRes))
      assertJSONEquals(expectedDate, getDisplayValue(dateRes))
      assertJSONEquals(expectedDateTime, getDisplayValue(dateTimeRes))
      assertJSONEquals(expectedNumber, getDisplayValue(numberRes))
      assertJSONEquals(expectedLink, getDisplayValueArray(linkRes))
    }
  }

}
