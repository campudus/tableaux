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
class CreateHistoryChangeDetectionTest extends TableauxTestBase with TestHelper {

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
}

@RunWith(classOf[VertxUnitRunner])
class CreateHistoryChangeLinkDetectionTest extends LinkTestBase with TestHelper {

  @Test
  def changeLinkValueTwice_singleLanguage(implicit c: TestContext): Unit = okTest {
    val twoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()

      _ = println(s"linkColumnId: $linkColumnId")
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", twoLinks)
      _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", twoLinks)
      rows <- sendRequest("GET", s"/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
    } yield {
      assertEquals(1, rows.size())
    }
  }

//   @Test
//   def changeLink_singleLanguageMultiIdentifiers(implicit c: TestContext): Unit = {
//     okTest {
//       val twoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         // Change target table structure to have a second identifier
//         _ <- sendRequest("POST", s"/tables/2/columns/2", Json.obj("identifier" -> true))

//         _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", twoLinks)
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//       } yield {
//         assertEquals(2, rows.size())
//       }
//     }
//   }
// }

// @RunWith(classOf[VertxUnitRunner])
// class CreateBidirectionalLinkHistoryTest extends LinkTestBase with TestHelper {

//   @Test
//   def changeLink_addOneLink(implicit c: TestContext): Unit = {
//     okTest {
//       val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(5)))

//       val linkTable = """[ {"id": 5, "value": "table2RowId3"} ]""".stripMargin
//       val targetLinkTable = """[ {"id": 1, "value": "table1row1"} ]""".stripMargin

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLink)

//         history <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         targetHistory <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)
//         historyLinks = getLinksValue(history, 0)
//         historyTargetLinks = getLinksValue(targetHistory, 0)
//       } yield {
//         JSONAssert.assertEquals(linkTable, historyLinks.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(targetLinkTable, historyTargetLinks.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_addTwoLinksAtOnce(implicit c: TestContext): Unit = {
//     okTest {
//       val putLink = Json.obj("value" -> Json.obj("values" -> Json.arr(4, 5)))

//       val linkTable = """[ {"id": 4, "value": "table2RowId2"},  {"id": 5, "value": "table2RowId3"} ]""".stripMargin
//       val targetLinkTable1 = """[ {"id": 1, "value": "table1row1"} ]""".stripMargin
//       val targetLinkTable2 = """[ {"id": 1, "value": "table1row1"} ]""".stripMargin

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLink)

//         history <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         targetHistory1 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         targetHistory2 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)
//         historyLinks = getLinksValue(history, 0)
//         historyTargetLinks1 = getLinksValue(targetHistory1, 0)
//         historyTargetLinks2 = getLinksValue(targetHistory2, 0)
//       } yield {
//         JSONAssert.assertEquals(linkTable, historyLinks.toString, JSONCompareMode.STRICT)
//         JSONAssert.assertEquals(targetLinkTable1, historyTargetLinks1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(targetLinkTable2, historyTargetLinks2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_addASecondLink(implicit c: TestContext): Unit = {
//     okTest {
//       val putInitialLink = Json.obj("value" -> Json.obj("values" -> Json.arr(5)))
//       val patchSecondLink = Json.obj("value" -> Json.obj("values" -> Json.arr(4)))

//       val linkTable =
//         """[
//           |  {"id": 5, "value": "table2RowId3"},
//           |  {"id": 4, "value": "table2RowId2"}
//           |]""".stripMargin

//       val backLink = """[ {"id": 1, "value": "table1row1"} ]""".stripMargin

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putInitialLink)
//         _ <- sendRequest("PATCH", s"/tables/1/columns/3/rows/1", patchSecondLink)

//         history <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         targetHistory1 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         targetHistory2 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//         historyLinks = getLinksValue(history, 1)
//         historyTargetLinks1 = getLinksValue(targetHistory1, 0)
//         historyTargetLinks2 = getLinksValue(targetHistory2, 0)
//       } yield {
//         JSONAssert.assertEquals(linkTable, historyLinks.toString, JSONCompareMode.STRICT)
//         JSONAssert.assertEquals(backLink, historyTargetLinks1.toString, JSONCompareMode.LENIENT)
//         assertEquals(1, targetHistory1.size())
//         JSONAssert.assertEquals(backLink, historyTargetLinks2.toString, JSONCompareMode.LENIENT)
//         assertEquals(1, targetHistory2.size())
//       }
//     }
//   }

//   @Test
//   def changeLink_oneLinkExisting_addTwoLinksAtOnce(implicit c: TestContext): Unit = {
//     okTest {
//       val putInitialLink = Json.obj("value" -> Json.obj("values" -> Json.arr(5)))
//       val patchSecondLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(4, 3)))

//       val linkTable =
//         """[
//           |  {"id": 5, "value": "table2RowId3"},
//           |  {"id": 4, "value": "table2RowId2"},
//           |  {"id": 3, "value": "table2RowId1"}
//           |]""".stripMargin

//       val backLink = """[ {"id": 1, "value": "table1row1"} ]""".stripMargin

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putInitialLink)
//         _ <- sendRequest("PATCH", s"/tables/1/columns/3/rows/1", patchSecondLinks)

//         history <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         targetHistoryRows1 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         targetHistoryRows2 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         targetHistoryRows3 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//         initialHistoryLinks = history.getJsonObject(0).getJsonArray("value")
//         historyLinks = history.getJsonObject(1).getJsonArray("value")
//         historyTargetLinks1 = targetHistoryRows1.getJsonObject(0).getJsonArray("value")
//         historyTargetLinks2 = targetHistoryRows2.getJsonObject(0).getJsonArray("value")
//         historyTargetLinks3 = targetHistoryRows3.getJsonObject(0).getJsonArray("value")
//       } yield {
//         assertEquals(1, initialHistoryLinks.size())
//         assertEquals(3, historyLinks.size())
//         JSONAssert.assertEquals(linkTable, historyLinks.toString, JSONCompareMode.STRICT)

//         assertEquals(1, targetHistoryRows1.size())
//         assertEquals(1, targetHistoryRows2.size())
//         assertEquals(1, targetHistoryRows3.size())

//         JSONAssert.assertEquals(backLink, historyTargetLinks1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(backLink, historyTargetLinks2.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(backLink, historyTargetLinks3.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_reverseOrder_doesNotChangeBackLinks(implicit c: TestContext): Unit = {
//     okTest {
//       val putLinks =
//         """
//           |{"value":
//           |  { "values": [3, 4, 5] }
//           |}
//           |""".stripMargin

//       val expected =
//         """
//           |[
//           |  {"id": 5, "value": "table2RowId3"},
//           |  {"id": 4, "value": "table2RowId2"},
//           |  {"id": 3, "value": "table2RowId1"}
//           |]
//         """.stripMargin

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", Json.fromObjectString(putLinks))

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1/link/3/order", Json.obj("location" -> "end"))
//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1/link/5/order", Json.obj("location" -> "start"))

//         backLinkHistory <- sendRequest("GET", "/tables/2/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         rows <- sendRequest("GET", s"/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         historyAfterCreation = getLinksValue(rows, 2)
//       } yield {
//         JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.STRICT_ORDER)
//         assertEquals(0, backLinkHistory.size())
//       }
//     }
//   }

//   @Test
//   def changeLink_threeExistingLinks_deleteOneLink(implicit c: TestContext): Unit = {
//     okTest {
//       val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(3, 4, 5)))

//       val expected =
//         """
//           |[
//           |  {"id": 3, "value": "table2RowId1"},
//           |  {"id": 5, "value": "table2RowId3"}
//           |]
//         """.stripMargin

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLinks)
//         _ <- sendRequest("DELETE", s"/tables/1/columns/3/rows/1/link/4")

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         historyAfterCreation = getLinksValue(rows, 1)
//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         backLinkRow5 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//       } yield {
//         JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.STRICT_ORDER)

//         assertEquals(1, backLinkRow3.size())
//         assertEquals(2, backLinkRow4.size())
//         assertEquals(1, backLinkRow5.size())

//         JSONAssert.assertEquals("""[{"id": 1}]""", getLinksValue(backLinkRow4, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow4, 1).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_threeExistingLinks_deleteCell(implicit c: TestContext): Unit = {
//     okTest {
//       val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(3, 4, 5)))

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLinks)
//         _ <- sendRequest("DELETE", s"/tables/1/columns/3/rows/1")

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         historyAfterCreation = getLinksValue(rows, 1)
//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         backLinkRow5 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//       } yield {
//         JSONAssert.assertEquals("[]", historyAfterCreation.toString, JSONCompareMode.LENIENT)

//         assertEquals(2, backLinkRow3.size())
//         assertEquals(2, backLinkRow4.size())
//         assertEquals(2, backLinkRow5.size())

//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow3, 1).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow4, 1).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow5, 1).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_threeExistingLinks_putOne(implicit c: TestContext): Unit = {
//     okTest {
//       val putLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(3, 4, 5)))
//       val putNewLink = Json.obj("value" -> Json.obj("values" -> Json.arr(4)))

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putLinks)
//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", putNewLink)

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         historyAfterCreation = getLinksValue(rows, 1)
//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         backLinkRow5 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//       } yield {
//         JSONAssert
//           .assertEquals(
//             """[{"id": 4, "value": "table2RowId2"}]""",
//             historyAfterCreation.toString,
//             JSONCompareMode.STRICT
//           )

//         assertEquals(2, backLinkRow3.size())
//         assertEquals(2, backLinkRow4.size())
//         assertEquals(2, backLinkRow5.size())

//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow3, 1).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(
//           """[{"id": 1, "value": "table1row1"}]""",
//           getLinksValue(backLinkRow4, 1).toString,
//           JSONCompareMode.STRICT
//         )
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow5, 1).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_twoLinksExistingInForeignTable_addAThirdLinkViaFirstTable(implicit c: TestContext): Unit = {
//     okTest {
//       val putInitialLinksFromTable1 = Json.obj("value" -> Json.obj("values" -> Json.arr(3, 4)))
//       val patchThirdLinkFromTable2 = Json.obj("value" -> Json.obj("values" -> Json.arr(1)))

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/2/columns/3/rows/1", putInitialLinksFromTable1)
//         _ <- sendRequest("PATCH", s"/tables/1/columns/3/rows/5", patchThirdLinkFromTable2)

//         linksTable2Rows <- sendRequest("GET", "/tables/2/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         linksTable1Rows3 <- sendRequest("GET", "/tables/1/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         linksTable1Rows4 <- sendRequest("GET", "/tables/1/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         linksTable1Rows5 <- sendRequest("GET", "/tables/1/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//       } yield {
//         assertEquals(1, linksTable1Rows3.size())
//         assertEquals(1, linksTable1Rows4.size())
//         assertEquals(1, linksTable1Rows5.size())

//         JSONAssert
//           .assertEquals(
//             """[{"id": 3}, {"id": 4}]""",
//             getLinksValue(linksTable2Rows, 0).toString,
//             JSONCompareMode.STRICT_ORDER
//           )
//         JSONAssert
//           .assertEquals(
//             """[{"id": 3}, {"id": 4}, {"id": 5}]""",
//             getLinksValue(linksTable2Rows, 1).toString,
//             JSONCompareMode.STRICT_ORDER
//           )
//       }
//     }
//   }

//   @Test
//   def changeLink_addThreeLinksFromAndToTable(implicit c: TestContext): Unit = {
//     okTest {
//       val putInitialLinksFromTable1 = Json.obj("value" -> Json.obj("values" -> Json.arr(3, 4, 5)))
//       val patchThirdLinkFromTable2 = Json.obj("value" -> Json.obj("values" -> Json.arr(3, 4, 5)))

//       val expectedLinksT1R3 =
//         """[
//           |  {"id": 4},
//           |  {"id": 5},
//           |  {"id": 3}
//           |]""".stripMargin

//       val expectedLinksT2R3 =
//         """[
//           |  {"id": 3},
//           |  {"id": 4},
//           |  {"id": 5}
//           |]""".stripMargin

//       val linkToTable2Row3 = """[{"id":3,"value":"table2RowId1"}]"""

//       val linkToTable1Row3 = """[{"id":3,"value":"table1RowId1"}]"""

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/3", putInitialLinksFromTable1)
//         _ <- sendRequest("PUT", s"/tables/2/columns/3/rows/3", patchThirdLinkFromTable2)

//         linksTable1Rows3 <- sendRequest("GET", "/tables/1/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         linksTable1Rows4 <- sendRequest("GET", "/tables/1/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         linksTable1Rows5 <- sendRequest("GET", "/tables/1/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//         linksTable2Rows3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         linksTable2Rows4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         linksTable2Rows5 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//       } yield {
//         assertEquals(2, linksTable1Rows3.size())
//         assertEquals(1, linksTable1Rows4.size())
//         assertEquals(1, linksTable1Rows5.size())

//         assertEquals(2, linksTable2Rows3.size())
//         assertEquals(1, linksTable2Rows4.size())
//         assertEquals(1, linksTable2Rows5.size())

//         JSONAssert
//           .assertEquals(expectedLinksT1R3, getLinksValue(linksTable1Rows3, 1).toString, JSONCompareMode.STRICT_ORDER)
//         JSONAssert.assertEquals(linkToTable2Row3, getLinksValue(linksTable1Rows4, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(linkToTable2Row3, getLinksValue(linksTable1Rows5, 0).toString, JSONCompareMode.LENIENT)

//         JSONAssert
//           .assertEquals(expectedLinksT2R3, getLinksValue(linksTable2Rows3, 1).toString, JSONCompareMode.STRICT_ORDER)
//         JSONAssert.assertEquals(linkToTable1Row3, getLinksValue(linksTable2Rows4, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(linkToTable1Row3, getLinksValue(linksTable2Rows5, 0).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }
// }

// @RunWith(classOf[VertxUnitRunner])
// class CreateMultiLanguageLinkHistoryTest extends LinkTestBase with TestHelper {

//   @Test
//   def changeLink_MultiIdentifiers_MultiLangAndSingleLangNumeric(implicit c: TestContext): Unit = {
//     okTest {

//       val expected =
//         """
//           |[
//           |  {"id":1,"value":{"de-DE":"Hallo, Table 2 Welt! 3.1415926","en-GB":"Hello, Table 2 World! 3.1415926"}},
//           |  {"id":2,"value":{"de-DE":"Hallo, Table 2 Welt2! 2.1415926","en-GB":"Hello, Table 2 World2! 2.1415926"}}
//           |]
//           |""".stripMargin

//       val postLinkColumn = Json.obj(
//         "columns" -> Json.arr(
//           Json.obj(
//             "name" -> "Test Link 1",
//             "kind" -> "link",
//             "toTable" -> 2
//           )
//         )
//       )
//       val putLinkValue = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

//       for {
//         _ <- createFullTableWithMultilanguageColumns("Table 1")
//         _ <- createFullTableWithMultilanguageColumns("Table 2")

//         // Change target table structure to have a second identifier
//         _ <- sendRequest("POST", s"/tables/2/columns/3", Json.obj("identifier" -> true))

//         // Add link column
//         linkColumn <- sendRequest("POST", s"/tables/1/columns", postLinkColumn)
//         linkColumnId = linkColumn.getJsonArray("columns").get[JsonObject](0).getNumber("id")

//         _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putLinkValue)
//         rows <- sendRequest("GET", "/tables/1/columns/8/rows/1/history?historyType=cell").map(toRowsArray)
//         historyAfterCreation = getLinksValue(rows, 0)
//       } yield {
//         JSONAssert.assertEquals(expected, historyAfterCreation.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }
// }

// @RunWith(classOf[VertxUnitRunner])
// class CreateHistoryCompatibilityTest extends LinkTestBase with TestHelper {
// // For migrated systems it is necessary to also write a history entry for a currently existing cell value

//   @Test
//   def changeSimpleValue_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {

//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val initialValue = """{ "value": "value before history feature" }"""
//       val firstChangedValue = """{ "value": "my first change with history feature" }"""

//       for {
//         _ <- createEmptyDefaultTable()
//         _ <- sendRequest("POST", "/tables/1/rows")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""UPDATE
//                                   |user_table_1
//                                   |SET column_1 = 'value before history feature'
//                                   |WHERE id = 1""".stripMargin)

//         _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", firstChangedValue)
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistoryCreation = rows.get[JsonObject](0)
//         firstHistoryCreation = rows.get[JsonObject](1)
//       } yield {
//         JSONAssert.assertEquals(initialValue, initialHistoryCreation.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(firstChangedValue, firstHistoryCreation.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeSimpleValue_secondChangeWithHistoryFeature_shouldAgainCreateSingleHistoryEntries(
//       implicit c: TestContext
//   ): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val initialValue = """{ "value": "value before history feature" }"""
//       val change1 = """{ "value": "first change" }"""
//       val change2 = """{ "value": "second change" }"""

//       for {
//         _ <- createEmptyDefaultTable()
//         _ <- sendRequest("POST", "/tables/1/rows")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""UPDATE
//                                   |user_table_1
//                                   |SET column_1 = 'value before history feature'
//                                   |WHERE id = 1""".stripMargin)

//         _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", change1)
//         _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", change2)
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.get[JsonObject](0)
//         history1 = rows.get[JsonObject](1)
//         history2 = rows.get[JsonObject](2)
//       } yield {
//         JSONAssert.assertEquals(initialValue, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(change1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(change2, history2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeMultilanguageValue_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(
//       implicit c: TestContext
//   ): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val initialValueDE = """{ "value": { "de-DE": "de-DE init" } }"""
//       val initialValueEN = """{ "value": { "en-GB": "en-GB init" } }"""
//       val change1 = """{ "value": { "de-DE": "de-DE first change" } }"""
//       val change2 = """{ "value": { "de-DE": "de-DE second change" } }"""
//       val change3 = """{ "value": { "en-GB": "en-GB first change" } }"""

//       for {
//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- sendRequest("POST", "/tables/1/rows")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""INSERT INTO user_table_lang_1(id, langtag,column_1)
//                                   |  VALUES
//                                   |(1, E'de-DE', E'de-DE init'),
//                                   |(1, E'en-GB', E'en-GB init')
//                                   |""".stripMargin)

//         _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", change1)
//         _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", change2)
//         _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", change3)
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistoryDE = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//         history2 = rows.getJsonObject(2)
//         initialHistoryEN = rows.getJsonObject(3)
//         history3 = rows.getJsonObject(4)
//       } yield {
//         JSONAssert.assertEquals(initialValueDE, initialHistoryDE.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(change1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(change2, history2.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(initialValueEN, initialHistoryEN.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(change3, history3.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeCurrencyPOST_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(
//       implicit c: TestContext
//   ): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedValues =
//         """[
//           |  {"value": {"DE": 11}},
//           |  {"value": {"GB": 22}},
//           |  {"value": {"DE": 33}},
//           |  {"value": {"GB": 44}}
//           |]""".stripMargin

//       val multiCountryCurrencyColumn = MultiCountry(CurrencyCol("currency-column"), Seq("DE", "GB"))

//       for {
//         _ <- createSimpleTableWithCell("table1", multiCountryCurrencyColumn)

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""INSERT INTO user_table_lang_1(id, langtag, column_1)
//                                   |  VALUES
//                                   |(1, E'DE', 11),
//                                   |(1, E'GB', 22)
//                                   |""".stripMargin)

//         _ <- sendRequest("POST", s"/tables/1/columns/1/rows/1", """{"value": {"DE": 33, "GB": 44}}""")
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)
//       } yield {
//         JSONAssert.assertEquals(expectedValues, rows.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeCurrencyPUT_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedValues =
//         """[
//           |  {"value": {"DE": 11}},
//           |  {"value": {"GB": 22}},
//           |  {"value": {"DE": 33}},
//           |  {"value": {"GB": 44}}
//           |]""".stripMargin

//       val multiCountryCurrencyColumn = MultiCountry(CurrencyCol("currency-column"), Seq("DE", "GB"))

//       for {
//         _ <- createSimpleTableWithCell("table1", multiCountryCurrencyColumn)

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""INSERT INTO user_table_lang_1(id, langtag, column_1)
//                                   |  VALUES
//                                   |(1, E'DE', 11),
//                                   |(1, E'GB', 22)
//                                   |""".stripMargin)

//         _ <- sendRequest("PUT", s"/tables/1/columns/1/rows/1", """{"value": {"DE": 33, "GB": 44}}""")
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)
//       } yield {
//         JSONAssert.assertEquals(expectedValues, rows.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLinkValue_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialLinks = """{ "value": [ {"id":3}, {"id": 4} ] }"""
//       val expectedAfterPostLinks = """{ "value": [ {"id":3}, {"id": 4}, {"id": 5} ] }"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4)
//                                   |  """.stripMargin)

//         _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", """{ "value": [ 5 ] }""")
//         rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)
//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks, history1.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLinkValue_twoLinkChanges_onlyFirstOneShouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialLinks = """{ "value": [ {"id":3} ] }"""
//       val expectedAfterPostLinks1 = """{ "value": [ {"id":3}, {"id":4} ] }"""
//       val expectedAfterPostLinks2 = """{ "value": [ {"id":3}, {"id":4}, {"id":5} ] }"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3)
//                                   |  """.stripMargin)

//         _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", """{ "value": [ 4 ] }""")
//         _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", """{ "value": [ 5 ] }""")
//         rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//         history2 = rows.getJsonObject(2)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks2, history2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteLinkValue_threeLinks_deleteOneOfThem(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialLinks = """{ "value": [ {"id":3}, {"id":4}, {"id":5} ] }"""
//       val expectedAfterPostLinks1 = """{ "value": [ {"id":3}, {"id":5} ] }"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4),
//                                   |  (1, 5)
//                                   |  """.stripMargin)

//         _ <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1/link/4")
//         rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks1, history1.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLinkOrder_reverseOrderInTwoSteps_createOnlyOneInitHistory(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialLinks = """{ "value": [ {"id":3}, {"id":4}, {"id":5} ] }"""
//       val expectedAfterPostLinks1 = """{ "value": [ {"id":4}, {"id":5}, {"id":3} ] }"""
//       val expectedAfterPostLinks2 = """{ "value": [ {"id":5}, {"id":4}, {"id":3} ] }"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4),
//                                   |  (1, 5)
//                                   |  """.stripMargin)

//         _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/3/order", s""" {"location": "end"} """)
//         _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1/link/5/order", s""" {"location": "start"} """)

//         rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//         history2 = rows.getJsonObject(2)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks2, history2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteLinkValue_threeLinks_deleteTwoTimesOnlyOneInitHistory(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialLinks = """{ "value": [ {"id":3}, {"id":4}, {"id":5} ] }"""
//       val expectedAfterPostLinks1 = """{ "value": [ {"id":3}, {"id":5} ] }"""
//       val expectedAfterPostLinks2 = """{ "value": [ {"id":5} ] }"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4),
//                                   |  (1, 5)
//                                   |  """.stripMargin)

//         _ <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1/link/4")
//         _ <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1/link/3")
//         rows <- sendRequest("GET", s"/tables/1/columns/$linkColumnId/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//         history2 = rows.getJsonObject(2)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPostLinks2, history2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeSimpleValue_booleanInitHistoryWithValueFalse(implicit c: TestContext): Unit = {
//     okTest {
//       // Booleans always gets a initial history entry on first change
//       val expectedInitialLinks = """{ "value": false} """
//       val expectedAfterPost1 = """{ "value": true }"""

//       val booleanColumn =
//         s"""{"columns": [{"kind": "boolean", "name": "Boolean Column", "languageType": "neutral"} ] }"""

//       for {
//         _ <- createEmptyDefaultTable("history test")

//         // create simple boolean column
//         _ <- sendRequest("POST", "/tables/1/columns", booleanColumn)

//         _ <- sendRequest("POST", "/tables/1/rows")
//         _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", expectedAfterPost1)
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPost1, history1.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeSimpleValue_booleanInitHistoryWithSameValue(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       // Booleans always gets a initial history entry on first change
//       val expectedInitialLinks = """{ "value": true} """
//       val expectedAfterPost1 = """{ "value": true }"""

//       val booleanColumn =
//         s"""{"columns": [{"kind": "boolean", "name": "Boolean Column", "languageType": "neutral"} ] }"""

//       for {
//         _ <- createEmptyDefaultTable("history test")

//         // create simple boolean column
//         _ <- sendRequest("POST", "/tables/1/columns", booleanColumn)

//         _ <- sendRequest("POST", "/tables/1/rows")

//         // manually update value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""UPDATE user_table_1 SET column_3 = TRUE WHERE id = 1""".stripMargin)

//         _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", expectedAfterPost1)
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPost1, history1.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeSimpleValue_boolean(implicit c: TestContext): Unit = {
//     okTest {
//       // Booleans always gets a initial history entry on first change
//       val expectedInitialLinks = """{ "value": false} """
//       val expectedAfterPost1 = """{ "value": true }"""
//       val expectedAfterPost2 = """{ "value": false }"""

//       val booleanColumn =
//         s"""{"columns": [{"kind": "boolean", "name": "Boolean Column", "languageType": "neutral"} ] }"""

//       for {
//         _ <- createEmptyDefaultTable("history test")

//         // create simple boolean column
//         _ <- sendRequest("POST", "/tables/1/columns", booleanColumn)

//         _ <- sendRequest("POST", "/tables/1/rows")
//         _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", expectedAfterPost1)
//         _ <- sendRequest("POST", "/tables/1/columns/3/rows/1", expectedAfterPost2)
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//         history2 = rows.getJsonObject(2)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPost1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPost2, history2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeMultilanguageValue_boolean(implicit c: TestContext): Unit = {
//     okTest {
//       // Booleans always gets a initial history entry on first change
//       val expectedInitialLinks = """{ "value": {"de-DE": false} }"""
//       val expectedAfterPost1 = """{ "value": {"de-DE": true} }"""
//       val expectedAfterPost2 = """{ "value": {"de-DE": false} }"""

//       for {
//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- sendRequest("POST", "/tables/1/rows")
//         _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", expectedAfterPost1)
//         _ <- sendRequest("POST", "/tables/1/columns/2/rows/1", expectedAfterPost2)
//         rows <- sendRequest("GET", "/tables/1/columns/2/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistory = rows.getJsonObject(0)
//         history1 = rows.getJsonObject(1)
//         history2 = rows.getJsonObject(2)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPost1, history1.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedAfterPost2, history2.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteSimpleCell_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {

//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val initialValue = """{ "value": "value before history feature" }"""

//       for {
//         _ <- createEmptyDefaultTable()
//         _ <- sendRequest("POST", "/tables/1/rows")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""UPDATE
//                                   |user_table_1
//                                   |SET column_1 = 'value before history feature'
//                                   |WHERE id = 1""".stripMargin)

//         _ <- sendRequest("DELETE", "/tables/1/columns/1/rows/1")
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history?historyType=cell").map(toRowsArray)

//         initialHistoryCreation = rows.get[JsonObject](0)
//         firstHistoryCreation = rows.get[JsonObject](1)
//       } yield {
//         JSONAssert.assertEquals(initialValue, initialHistoryCreation.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("""{ "value": null }""", firstHistoryCreation.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteMultilanguageCell_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(
//       implicit c: TestContext
//   ): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val initialValueDE = """{ "value": { "de-DE": "de-DE init" } }"""

//       for {
//         _ <- createTableWithMultilanguageColumns("history test")
//         _ <- sendRequest("POST", "/tables/1/rows")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""INSERT INTO user_table_lang_1(id, langtag, column_1)
//                                   |  VALUES
//                                   |(1, E'de-DE', E'de-DE init'),
//                                   |(1, E'en-GB', E'en-GB init')
//                                   |""".stripMargin)

//         _ <- sendRequest("DELETE", "/tables/1/columns/1/rows/1")
//         rows <- sendRequest("GET", "/tables/1/columns/1/rows/1/history/de-DE?historyType=cell").map(toRowsArray)

//         initialHistoryDE = rows.getJsonObject(0)
//         history = rows.getJsonObject(1)
//       } yield {
//         JSONAssert.assertEquals(initialValueDE, initialHistoryDE.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("""{ "value": { "de-DE": null } }""", history.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteLinkCell_threeLinks_deleteTwoTimesOnlyOneInitHistory(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialLinks = """[ {"id":3}, {"id":4}, {"id":5} ]"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4),
//                                   |  (1, 5)
//                                   |  """.stripMargin)

//         _ <- sendRequest("DELETE", s"/tables/1/columns/$linkColumnId/rows/1")
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         initialHistory = getLinksValue(rows, 0)
//         history = getLinksValue(rows, 1)
//       } yield {
//         JSONAssert.assertEquals(expectedInitialLinks, initialHistory.toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", history.toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def addAttachment_firstChangeWithHistoryFeature_shouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query(s"""INSERT INTO system_attachment
//                                    |  (table_id, column_id, row_id, attachment_uuid, ordering)
//                                    |VALUES
//                                    |  (1, 3, 1, '$fileUuid1', 1)
//                                    |  """.stripMargin)

//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("uuid" -> fileUuid2)))
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         initialHistory = rows.get[JsonObject](0).getJsonArray("value").getJsonObject(0)
//         history = rows.get[JsonObject](1).getJsonArray("value").getJsonObject(0)
//       } yield {
//         assertJSONEquals(Json.obj("uuid" -> fileUuid1), initialHistory, JSONCompareMode.LENIENT)
//         assertJSONEquals(Json.obj("uuid" -> fileUuid1), history, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def addAttachment_twoTimes_shouldOnlyCreateOneInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")
//         fileUuid3 <- createTestAttachment("Test 3")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query(s"""INSERT INTO system_attachment
//                                    |  (table_id, column_id, row_id, attachment_uuid, ordering)
//                                    |VALUES
//                                    |  (1, 3, 1, '$fileUuid1', 1)
//                                    |  """.stripMargin)

//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("uuid" -> fileUuid2)))
//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("uuid" -> fileUuid3)))
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//       } yield {
//         assertEquals(3, rows.size())
//       }
//     }
//   }

//   @Test
//   def deleteAttachmentCell_shouldCreateInitialHistoryEntry(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")

//         // manually insert a value that simulates cell value changes before implementation of the history feature
//         _ <- dbConnection.query(s"""INSERT INTO system_attachment
//                                    |  (table_id, column_id, row_id, attachment_uuid, ordering)
//                                    |VALUES
//                                    |  (1, 3, 1, '$fileUuid1', 1),
//                                    |  (1, 3, 1, '$fileUuid2', 2)
//                                    |  """.stripMargin)

//         _ <- sendRequest("DELETE", "/tables/1/columns/3/rows/1")

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         initialCountBeforeDeletion = rows.get[JsonObject](0).getJsonArray("value")
//         attachmentCountAfterDeletion = rows.get[JsonObject](1).getJsonArray("value")
//       } yield {
//         assertEquals(2, initialCountBeforeDeletion.size())
//         assertEquals(0, attachmentCountAfterDeletion.size())
//       }
//     }
//   }

// }

// @RunWith(classOf[VertxUnitRunner])
// class CreateAttachmentHistoryTest extends MediaTestBase with TestHelper {

//   @Test
//   def addAttachment_toEmptyCell(implicit c: TestContext): Unit = {
//     okTest {
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid <- createTestAttachment("Test 1")

//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("uuid" -> fileUuid)))
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         currentUuid = rows.get[JsonObject](0).getJsonArray("value").getJsonObject(0).getString("uuid")
//       } yield {
//         assertEquals(fileUuid, currentUuid)
//       }
//     }
//   }

//   @Test
//   def addAttachment_toCellContainingOneAttachment(implicit c: TestContext): Unit = {
//     okTest {
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")

//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("uuid" -> fileUuid1)))
//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.obj("uuid" -> fileUuid2)))
//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         firstHistory = rows.get[JsonObject](0).getJsonArray("value").getJsonObject(0)
//         secondHistory1 = rows.get[JsonObject](1).getJsonArray("value").getJsonObject(0)
//         secondHistory2 = rows.get[JsonObject](1).getJsonArray("value").getJsonObject(1)
//       } yield {
//         assertJSONEquals(Json.obj("uuid" -> fileUuid1), firstHistory, JSONCompareMode.LENIENT)
//         assertJSONEquals(Json.obj("uuid" -> fileUuid1), secondHistory1, JSONCompareMode.LENIENT)
//         assertJSONEquals(Json.obj("uuid" -> fileUuid2), secondHistory2, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def addAttachments_addThreeAttachmentsAtOnce(implicit c: TestContext): Unit = {
//     okTest {
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")

//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.arr(fileUuid1, fileUuid2)))

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         history1 = rows.get[JsonObject](0).getJsonArray("value").getJsonObject(0)
//         history2 = rows.get[JsonObject](0).getJsonArray("value").getJsonObject(1)
//       } yield {
//         assertJSONEquals(Json.obj("uuid" -> fileUuid1), history1, JSONCompareMode.LENIENT)
//         assertJSONEquals(Json.obj("uuid" -> fileUuid2), history2, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteAttachment_fromCellContainingTwoAttachments(implicit c: TestContext): Unit = {
//     okTest {
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")

//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.arr(fileUuid1, fileUuid2)))
//         _ <- sendRequest("DELETE", s"/tables/1/columns/3/rows/1/attachment/$fileUuid1")

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         afterDeletionHistory = rows.get[JsonObject](1).getJsonArray("value").getJsonObject(0)
//       } yield {

//         assertJSONEquals(Json.obj("uuid" -> fileUuid2), afterDeletionHistory, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteAttachment_fromCellContainingThreeAttachments(implicit c: TestContext): Unit = {
//     okTest {
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")
//         fileUuid3 <- createTestAttachment("Test 3")

//         _ <- sendRequest(
//           "POST",
//           s"/tables/1/columns/3/rows/1",
//           Json.obj("value" -> Json.arr(fileUuid1, fileUuid2, fileUuid3))
//         )
//         _ <- sendRequest("DELETE", s"/tables/1/columns/3/rows/1/attachment/$fileUuid2")

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)

//         afterDeletionHistory1 = rows.get[JsonObject](1).getJsonArray("value").getJsonObject(0)
//         afterDeletionHistory2 = rows.get[JsonObject](1).getJsonArray("value").getJsonObject(1)
//       } yield {
//         assertJSONEquals(Json.obj("uuid" -> fileUuid1), afterDeletionHistory1, JSONCompareMode.LENIENT)
//         assertJSONEquals(Json.obj("uuid" -> fileUuid3), afterDeletionHistory2, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def deleteCell_attachment(implicit c: TestContext): Unit = {
//     okTest {
//       val attachmentColumn = """{"columns": [{"kind": "attachment", "name": "Downloads"}] }"""

//       for {
//         _ <- createDefaultTable()
//         _ <- sendRequest("POST", s"/tables/1/columns", attachmentColumn)

//         fileUuid1 <- createTestAttachment("Test 1")
//         fileUuid2 <- createTestAttachment("Test 2")
//         _ <- sendRequest("POST", s"/tables/1/columns/3/rows/1", Json.obj("value" -> Json.arr(fileUuid1, fileUuid2)))

//         _ <- sendRequest("DELETE", "/tables/1/columns/3/rows/1")

//         rows <- sendRequest("GET", "/tables/1/columns/3/rows/1/history?historyType=cell").map(toRowsArray)
//         attachmentCountBeforeDeletion = rows.get[JsonObject](0).getJsonArray("value")
//         attachmentCountAfterDeletion = rows.get[JsonObject](1).getJsonArray("value")
//       } yield {
//         assertEquals(2, attachmentCountBeforeDeletion.size())
//         assertEquals(0, attachmentCountAfterDeletion.size())
//       }
//     }
//   }
// }

// @RunWith(classOf[VertxUnitRunner])
// class CreateBidirectionalCompatibilityLinkHistoryTest extends LinkTestBase with TestHelper {

//   @Test
//   def changeLink_twoLinksExisting_postOtherLink(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedBacklink = """[{"id": 1, "value": "table1row1"}]"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4)
//                                   |  """.stripMargin)

//         _ <- sendRequest("POST", s"/tables/1/columns/$linkColumnId/rows/1", """{ "value": [ 5 ] }""")
//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         backLinkRow5 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)

//       } yield {
//         assertEquals(1, backLinkRow3.size())
//         assertEquals(1, backLinkRow4.size())
//         assertEquals(1, backLinkRow5.size())

//         JSONAssert.assertEquals(expectedBacklink, getLinksValue(backLinkRow3, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedBacklink, getLinksValue(backLinkRow4, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedBacklink, getLinksValue(backLinkRow5, 0).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_twoLinksExisting_putOneOfThem(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedBacklink = """[{"id": 1, "value": "table1row1"}]"""

//       for {
//         linkColumnId <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4)
//                                   |  """.stripMargin)

//         _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", """{ "value": [ 4 ] }""")
//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//       } yield {
//         assertEquals(2, backLinkRow3.size())
//         assertEquals(2, backLinkRow4.size())

//         JSONAssert.assertEquals(expectedBacklink, getLinksValue(backLinkRow3, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow3, 1).toString, JSONCompareMode.LENIENT)

//         JSONAssert.assertEquals(expectedBacklink, getLinksValue(backLinkRow4, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals(expectedBacklink, getLinksValue(backLinkRow4, 1).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLink_twoLinksExisting_putEmptyLinkArray(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       val expectedInitialBacklink = """[{"id": 1, "value": "table1row1"}]"""

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4)
//                                   |  """.stripMargin)

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1", """{ "value": [] }""")
//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//       } yield {
//         assertEquals(2, backLinkRow3.size())
//         assertEquals(2, backLinkRow4.size())

//         JSONAssert
//           .assertEquals(expectedInitialBacklink, getLinksValue(backLinkRow3, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow3, 1).toString, JSONCompareMode.LENIENT)

//         JSONAssert
//           .assertEquals(expectedInitialBacklink, getLinksValue(backLinkRow4, 0).toString, JSONCompareMode.LENIENT)
//         JSONAssert.assertEquals("[]", getLinksValue(backLinkRow4, 1).toString, JSONCompareMode.LENIENT)
//       }
//     }
//   }

//   @Test
//   def changeLinkOrder_reverseOrderInTwoSteps_shouldNotCreateBacklinkHistory(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (1, 3),
//                                   |  (1, 4),
//                                   |  (1, 5)
//                                   |  """.stripMargin)

//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1/link/3/order", s""" {"location": "end"} """)
//         _ <- sendRequest("PUT", s"/tables/1/columns/3/rows/1/link/5/order", s""" {"location": "start"} """)

//         backLinkRow3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         backLinkRow4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//       } yield {
//         assertEquals(1, backLinkRow3.size())
//         assertEquals(1, backLinkRow4.size())
//       }
//     }
//   }

//   @Test
//   def patchLink_oneLinksExisting_deleteCell(implicit c: TestContext): Unit = {
//     okTest {
//       val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//       val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

//       for {
//         _ <- setupTwoTablesWithEmptyLinks()

//         _ <- dbConnection.query("""INSERT INTO link_table_1
//                                   |  (id_1, id_2)
//                                   |VALUES
//                                   |  (3, 3), (3, 4), (4, 3)
//                                   |  """.stripMargin)

//         _ <- sendRequest("PATCH", s"/tables/1/columns/3/rows/3", """{ "value": [ 5 ] }""")

//         t1r3 <- sendRequest("GET", "/tables/1/columns/3/rows/3/history?historyType=cell").map(toRowsArray)

//         t2r3 <- sendRequest("GET", "/tables/2/columns/3/rows/3/history?historyType=cell").map(toRowsArray)
//         t2r4 <- sendRequest("GET", "/tables/2/columns/3/rows/4/history?historyType=cell").map(toRowsArray)
//         t2r5 <- sendRequest("GET", "/tables/2/columns/3/rows/5/history?historyType=cell").map(toRowsArray)
//       } yield {
//         assertEquals(1, t2r3.size())
//         assertEquals(1, t2r4.size())
//         assertEquals(1, t2r5.size())

//         assertEquals(2, t1r3.size())

//         assertJSONEquals("""[{"id": 3}, {"id": 4}]""", getLinksValue(t2r3, 0).toString, JSONCompareMode.STRICT_ORDER)
//         assertJSONEquals("""[{"id": 3}]""", getLinksValue(t2r4, 0).toString, JSONCompareMode.STRICT_ORDER)
//         assertJSONEquals("""[{"id": 3}]""", getLinksValue(t2r5, 0).toString, JSONCompareMode.STRICT_ORDER)

//         assertJSONEquals("""[{"id": 3}, {"id": 4}]""", getLinksValue(t1r3, 0).toString, JSONCompareMode.STRICT_ORDER)
//         assertJSONEquals(
//           """[{"id": 3}, {"id": 4}, {"id": 5}]""",
//           getLinksValue(t1r3, 1).toString,
//           JSONCompareMode.STRICT_ORDER
//         )
//       }
//     }
//   }

}
