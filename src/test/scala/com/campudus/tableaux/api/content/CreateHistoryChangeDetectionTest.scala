package com.campudus.tableaux.api.content

import com.campudus.tableaux.api.media.MediaTestBase
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.testtools.RequestCreation.{CurrencyCol, MultiCountry, Rows}
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.core.json.JsonArray
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

import org.junit.{Ignore, Test}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

@RunWith(classOf[VertxUnitRunner])
class CreateHistoryChangeDetectionTest extends LinkTestBase with TestHelper {

  @Test
  def changeSimpleValueTwice_changeACellOnlyOnce(implicit c: TestContext): Unit = {
    okTest {
      val newValue = Json.obj("value" -> "a fix value")

      for {
        _ <- createEmptyDefaultTable()
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", newValue)
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", newValue)
        rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)
      } yield {
        println(s"rows: $rows")
        assertEquals(1, rows.size())
      }
    }
  }

  @Test
  def changeMultilanguageValueTwice_text(implicit c: TestContext): Unit = {
    okTest {
      val newValue = Json.obj("value" -> Json.obj("de-DE" -> "a fix value"))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("PATCH", "/tables/1/columns/1/rows/1", newValue)
        _ <- sendRequest("PATCH", "/tables/1/columns/1/rows/1", newValue)
        rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)
      } yield {
        assertEquals(1, rows.size())
      }
    }
  }

  @Test
  def changeMultilanguageValueTwice_boolean(implicit c: TestContext): Unit = {
    okTest {
      val newValue = Json.obj("value" -> Json.obj("de-DE" -> true))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        // Booleans always gets a initial history entry on first change, so +1 history row
        _ <- sendRequest("PATCH", "/tables/1/columns/2/rows/1", newValue)
        _ <- sendRequest("PATCH", "/tables/1/columns/2/rows/1", newValue)
        rows <- sendRequest("GET", "/tables/1/columns/2/rows/1/history?historyType=cell").map(toRowsArray)
      } yield {
        assertEquals(2, rows.size())
      }
    }
  }

  @Test
  def changeMultilanguageValueTwice_numeric(implicit c: TestContext): Unit = {
    okTest {
      val newValue = Json.obj("value" -> Json.obj("de-DE" -> 42))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("PATCH", "/tables/1/columns/3/rows/1", newValue)
        _ <- sendRequest("PATCH", "/tables/1/columns/3/rows/1", newValue)
        rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
      } yield {
        assertEquals(1, rows.size())
      }
    }
  }

  @Test
  def changeMultilanguageValueTwice_datetime(implicit c: TestContext): Unit = {
    okTest {
      val newValue = Json.obj("value" -> Json.obj("de-DE" -> "2019-01-18T00:00:00.000Z"))

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("PATCH", "/tables/1/columns/7/rows/1", newValue)
        _ <- sendRequest("PATCH", "/tables/1/columns/7/rows/1", newValue)
        rows <- sendRequest("GET", "/tables/1/columns/7/rows/1/history?historyType=cell").map(toRowsArray)
      } yield {
        assertEquals(1, rows.size())
      }
    }
  }

  @Test
  def changeMultilanguageValueTwice_currency(implicit c: TestContext): Unit = {
    okTest {
      val newValue = Json.obj("value" -> Json.obj("DE" -> 2999.99))
      val multiCountryCurrencyColumn = MultiCountry(CurrencyCol("currency-column"), Seq("DE", "GB"))

      for {
        _ <- createSimpleTableWithCell("table1", multiCountryCurrencyColumn)
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("PATCH", "/tables/1/columns/1/rows/1", newValue)
        _ <- sendRequest("PATCH", "/tables/1/columns/1/rows/1", newValue)
        rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)
      } yield {
        assertEquals(1, rows.size())
      }
    }
  }

  @Test
  def changeLinkValueTwice_singleLanguage(implicit c: TestContext): Unit = okTest {
    val twoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      _ = println(s"linkColumnId: $linkColumnId")
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", twoLinks)
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", twoLinks)
      rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)
    } yield {
      assertEquals(1, rows.size())
    }
  }

  @Test
  def changeLinkValueTwice_singleLanguageMultiIdentifiers(implicit c: TestContext): Unit = okTest {
    val twoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      // Change target table structure to have a second identifier
      _ <- sendRequest("POST", s"/tables/2/columns/2", Json.obj("identifier" -> true))

      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", twoLinks)
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", twoLinks)
      rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)
    } yield {
      assertEquals(1, rows.size())
    }
  }

  @Test
  def changeLinkValueTwice_addOne(implicit c: TestContext): Unit = okTest {
    val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(5)))

    for {
      _ <- setupTwoTablesWithEmptyLinks()

      _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLink)
      _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLink)

      history <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
      targetHistory <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)
    } yield {
      assertEquals(1, history.size())
      assertEquals(1, targetHistory.size())
    }
  }

  @Test
  def changeLink_addTwoLinksAtOnce(implicit c: TestContext): Unit = {
    okTest {
      val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(4, 5)))

      for {
        _ <- setupTwoTablesWithEmptyLinks()

        _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLink)
        _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLink)

        history <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
        targetHistory1 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
        targetHistory2 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)
      } yield {
        assertEquals(1, history.size())
        assertEquals(1, targetHistory1.size())
        assertEquals(1, targetHistory2.size())
      }
    }
  }
}
