package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation.{CurrencyCol, MultiCountry}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class CreateHistoryTest extends TableauxTestBase {

  @Test
  def changeSimpleValue_historyAfterDefaultTableCreation(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |{
          |  "event": "cell_changed",
          |  "columnType": "text",
          |  "languageType": "neutral",
          |  "value": "table1row1"
          |}
        """.stripMargin

      for {
        _ <- createDefaultTable()
        test <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows").get[JsonObject](0)
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeSimpleValue_changeACellInEmptyRow(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |{
          |  "event": "cell_changed",
          |  "columnType": "text",
          |  "languageType": "neutral",
          |  "value": "my first change"
          |}
        """.stripMargin

      val newValue = Json.obj("value" -> "my first change")

      for {
        _ <- createEmptyDefaultTable()
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", newValue)
        test <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows").get[JsonObject](0)
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeSimpleValue_changeACellMultipleTimes(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[
          |  {
          |    "event": "cell_changed",
          |    "columnType": "numeric",
          |    "languageType": "neutral",
          |    "value": 42
          |  }, {
          |    "event": "cell_changed",
          |    "columnType": "numeric",
          |    "languageType": "neutral",
          |    "value": 1337
          |  },{
          |    "event": "cell_changed",
          |    "columnType": "numeric",
          |    "languageType": "neutral",
          |    "value": 1123581321
          |  }
          |]
        """.stripMargin

      for {
        _ <- createEmptyDefaultTable()
        _ <- sendRequest("POST", "/tables/1/rows")

        _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", Json.obj("value" -> 42))
        _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", Json.obj("value" -> 1337))
        _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", Json.obj("value" -> 1123581321))
        history <- sendRequest("GET", "/tables/1/columns/2/rows/1/history")
        historyRows = history.getJsonArray("rows")
      } yield {
        JSONAssert.assertEquals(expected, historyRows.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeMultilanguageValue_text(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[
          |  {
          |    "event": "cell_changed",
          |    "columnType": "text",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": "first change"
          |    }
          |  }, {
          |    "event": "cell_changed",
          |    "columnType": "text",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": "second change"
          |    }
          |  }
          |]
        """.stripMargin

      val newValue1 = Json.obj("value" -> Json.obj("de-DE" -> "first change"))
      val newValue2 = Json.obj("value" -> Json.obj("de-DE" -> "second change"))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", newValue1)
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", newValue2)
        test <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows")
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeMultilanguageValue_boolean(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[
          |  {
          |    "event": "cell_changed",
          |    "columnType": "boolean",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": true
          |    }
          |  }, {
          |    "event": "cell_changed",
          |    "columnType": "boolean",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": false
          |    }
          |  }
          |]
        """.stripMargin

      val newValue1 = Json.obj("value" -> Json.obj("de-DE" -> true))
      val newValue2 = Json.obj("value" -> Json.obj("de-DE" -> false))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", newValue1)
        _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", newValue2)
        test <- sendRequest("GET", "/tables/1/columns/2/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows")
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeMultilanguageValue_numeric(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[
          |  {
          |    "event": "cell_changed",
          |    "columnType": "numeric",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": 42
          |    }
          |  }, {
          |    "event": "cell_changed",
          |    "columnType": "numeric",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": 1337
          |    }
          |  }
          |]
        """.stripMargin

      val newValue1 = Json.obj("value" -> Json.obj("de-DE" -> 42))
      val newValue2 = Json.obj("value" -> Json.obj("de-DE" -> 1337))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", newValue1)
        _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", newValue2)
        test <- sendRequest("GET", "/tables/1/columns/3/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows")
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeMultilanguageValue_datetime(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[
          |  {
          |    "event": "cell_changed",
          |    "columnType": "datetime",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": "2019-01-18T00:00:00.000Z"
          |    }
          |  }, {
          |    "event": "cell_changed",
          |    "columnType": "datetime",
          |    "languageType": "language",
          |    "value": {
          |      "de-DE": "2018-12-12T00:00:00.000Z"
          |    }
          |  }
          |]
        """.stripMargin

      val newValue1 = Json.obj("value" -> Json.obj("de-DE" -> "2019-01-18T00:00:00.000Z"))
      val newValue2 = Json.obj("value" -> Json.obj("de-DE" -> "2018-12-12T00:00:00.000Z"))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/7/rows/1", newValue1)
        _ <- sendRequest("POST", "/tables/1/columns/7/rows/1", newValue2)
        test <- sendRequest("GET", "/tables/1/columns/7/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows")
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def changeMultilanguageValue_currency(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[
          |  {
          |    "event": "cell_changed",
          |    "columnType": "currency",
          |    "languageType": "country",
          |    "value": {
          |      "DE": 2999.99
          |    }
          |  }, {
          |    "event": "cell_changed",
          |    "columnType": "currency",
          |    "languageType": "country",
          |    "value": {
          |      "DE": 4000
          |    }
          |  }
          |]
        """.stripMargin

      val multiCountryCurrencyColumn = MultiCountry(CurrencyCol("currency-column"), Seq("DE", "GB"))

      for {
        _ <- createSimpleTableWithCell("table1", multiCountryCurrencyColumn)

        _ <- sendRequest("PUT", s"/tables/1/columns/1/rows/1", Json.obj("value" -> Json.obj("DE" -> 2999.99)))
        _ <- sendRequest("PUT", s"/tables/1/columns/1/rows/1", Json.obj("value" -> Json.obj("DE" -> 4000)))
        test <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
        historyAfterCreation = test.getJsonArray("rows")
      } yield {
        JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
      }
    }
  }
}
