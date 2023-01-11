package com.campudus.tableaux.api.content

import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class FillCellTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))

  val createNumberColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  val createCurrencyColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  val createBooleanColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "boolean", "name" -> "Test Column 3")))
  val createDateColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "date", "name" -> "Test Column 4")))

  val createDateTimeColumnJson =
    Json.obj("columns" -> Json.arr(Json.obj("kind" -> "datetime", "name" -> "Test Column 5")))

  @Test
  def fillSingleCellWithNull(implicit c: TestContext): Unit = okTest {
    val fillStringCellJson = Json.obj("value" -> null)

    val expectedCell = Json.obj("status" -> "ok", "value" -> null)

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      fillResult <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson)
      cellResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, fillResult)
      assertEquals(expectedCell, cellResult)
    }
  }

  @Test
  def fillSingleStringCell(implicit c: TestContext): Unit = okTest {
    val fillStringCellJson = Json.obj("value" -> "Test Fill 1")

    val expectedCell = Json.obj("status" -> "ok", "value" -> "Test Fill 1")

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson)
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, test)
      assertEquals(expectedCell, getResult)
    }
  }

  @Test
  def fillStringCellWithLengthInRange(implicit c: TestContext): Unit = okTest {
    val fillStringCellJson1 = Json.obj("value" -> "12345")
    val fillStringCellJson2 = Json.obj("value" -> "1234567")
    val fillStringCellJson3 = Json.obj("value" -> "0123456789")
    val fillStringCellJson4 = Json.obj("value" -> Json.obj("de" -> "12345", "en" -> "0123456789"))

    val expectedCell1 = Json.obj("status" -> "ok", "value" -> "12345")
    val expectedCell2 = Json.obj("status" -> "ok", "value" -> "1234567")
    val expectedCell3 = Json.obj("status" -> "ok", "value" -> "0123456789")
    val expectedCell4 = Json.obj("status" -> "ok", "value" -> Json.obj("de" -> "12345", "en" -> "0123456789"))

    val createStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "text",
      "name" -> "Test Column 1",
      "attributes" -> Json.obj(
        "minLength" -> Json.obj(
          "value" -> 5,
          "type" -> "number"
        ),
        "maxLength" -> Json.obj(
          "value" -> 10,
          "type" -> "number"
        )
      )
    )))

    val createMultilangStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "text",
      "name" -> "Test Column 2",
      "multilanguage" -> true,
      "attributes" -> Json.obj(
        "minLength" -> Json.obj(
          "value" -> 5,
          "type" -> "number"
        ),
        "maxLength" -> Json.obj(
          "value" -> 10,
          "type" -> "number"
        )
      )
    )))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJsonWithLengthLimit)
      _ <- sendRequest("POST", "/tables/1/columns", createMultilangStringColumnJsonWithLengthLimit)
      _ <- sendRequest("POST", "/tables/1/rows")
      res1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson1)
      res2 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson2)
      res3 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson3)
      res4 <- sendRequest("POST", "/tables/1/columns/2/rows/1", fillStringCellJson4)
    } yield {
      assertEquals(expectedCell1, res1)
      assertEquals(expectedCell2, res2)
      assertEquals(expectedCell3, res3)
      assertEquals(expectedCell4, res4)
    }
  }

  @Test
  def emptyStringCellWithLimitsPresent(implicit c: TestContext): Unit = okTest {
    val fillStringCellJson1 = Json.obj("value" -> "12345")
    val fillStringCellJson2 = Json.obj("value" -> "")
    val fillStringCellJson3 = Json.obj("value" -> Json.obj("de" -> "123456", "en" -> "0123456789"))
    val fillStringCellJson4 = Json.obj("value" -> Json.obj("de" -> "", "en" -> "0123456789"))

    val expectedCell2 = Json.obj("status" -> "ok", "value" -> "")
    val expectedCell4 = Json.obj("status" -> "ok", "value" -> Json.obj("de" -> "", "en" -> "0123456789"))

    val createStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "text",
      "name" -> "Test Column 1",
      "attributes" -> Json.obj(
        "minLength" -> Json.obj(
          "value" -> 5,
          "type" -> "number"
        ),
        "maxLength" -> Json.obj(
          "value" -> 10,
          "type" -> "number"
        )
      )
    )))

    val createMultilangStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
      "kind" -> "text",
      "name" -> "Test Column 2",
      "multilanguage" -> true,
      "attributes" -> Json.obj(
        "minLength" -> Json.obj(
          "value" -> 5,
          "type" -> "number"
        ),
        "maxLength" -> Json.obj(
          "value" -> 10,
          "type" -> "number"
        )
      )
    )))

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJsonWithLengthLimit)
      _ <- sendRequest("POST", "/tables/1/columns", createMultilangStringColumnJsonWithLengthLimit)
      _ <- sendRequest("POST", "/tables/1/rows")
      res1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson1)
      res2 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson2)
      res3 <- sendRequest("POST", "/tables/1/columns/2/rows/1", fillStringCellJson3)
      res4 <- sendRequest("POST", "/tables/1/columns/2/rows/1", fillStringCellJson4)
    } yield {
      assertEquals(expectedCell2, res2)
      assertEquals(expectedCell4, res4)
    }
  }

  @Test
  def fillSingleStringCellWithLengthTooShort(implicit c: TestContext): Unit =
    exceptionTest("error.request.value.length") {
      val fillStringCellJson1 = Json.obj("value" -> "1234")

      val expectedCell1 = Json.obj("status" -> "ok", "value" -> "12345")

      val createStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
        "kind" -> "text",
        "name" -> "Test Column 1",
        "attributes" -> Json.obj(
          "minLength" -> Json.obj(
            "value" -> 5,
            "type" -> "number"
          ),
          "maxLength" -> Json.obj(
            "value" -> 10,
            "type" -> "number"
          )
        )
      )))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        col <- sendRequest("POST", "/tables/1/columns", createStringColumnJsonWithLengthLimit)
        _ <- sendRequest("POST", "/tables/1/rows")
        res1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson1)
      } yield ()
    }

  @Test
  def fillMultilangStringCellWithLengthTooShort(implicit c: TestContext): Unit =
    exceptionTest("error.request.value.length") {
      val fillStringCellJson1 = Json.obj("value" -> Json.obj("de" -> "123", "en" -> "123456"))

      val createStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
        "kind" -> "text",
        "name" -> "Test Column 1",
        "multilanguage" -> true,
        "attributes" -> Json.obj(
          "minLength" -> Json.obj(
            "value" -> 5,
            "type" -> "number"
          ),
          "maxLength" -> Json.obj(
            "value" -> 10,
            "type" -> "number"
          )
        )
      )))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        col <- sendRequest("POST", "/tables/1/columns", createStringColumnJsonWithLengthLimit)
        _ <- sendRequest("POST", "/tables/1/rows")
        res1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson1)
      } yield ()
    }

  @Test
  def fillMultilangStringCellWithLengthTooLong(implicit c: TestContext): Unit =
    exceptionTest("error.request.value.length") {
      val fillStringCellJson1 = Json.obj("value" -> Json.obj("de" -> "12345678912345", "en" -> "123456"))

      val createStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
        "kind" -> "text",
        "name" -> "Test Column 1",
        "multilanguage" -> true,
        "attributes" -> Json.obj(
          "minLength" -> Json.obj(
            "value" -> 5,
            "type" -> "number"
          ),
          "maxLength" -> Json.obj(
            "value" -> 10,
            "type" -> "number"
          )
        )
      )))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        col <- sendRequest("POST", "/tables/1/columns", createStringColumnJsonWithLengthLimit)
        _ <- sendRequest("POST", "/tables/1/rows")
        res1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson1)
      } yield ()
    }

  @Test
  def fillSingleStringCellWithLengthTooLong(implicit c: TestContext): Unit =
    exceptionTest("error.request.value.length") {
      val fillStringCellJson1 = Json.obj("value" -> "012345678921")

      val expectedCell1 = Json.obj("status" -> "ok", "value" -> "12345")

      val createStringColumnJsonWithLengthLimit = Json.obj("columns" -> Json.arr(Json.obj(
        "kind" -> "text",
        "name" -> "Test Column 1",
        "attributes" -> Json.obj(
          "minLength" -> Json.obj(
            "value" -> 5,
            "type" -> "number"
          ),
          "maxLength" -> Json.obj(
            "value" -> 10,
            "type" -> "number"
          )
        )
      )))

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        col <- sendRequest("POST", "/tables/1/columns", createStringColumnJsonWithLengthLimit)
        _ <- sendRequest("POST", "/tables/1/rows")
        res1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillStringCellJson1)
      } yield ()
    }

  @Test
  def fillSingleNumberCell(implicit c: TestContext): Unit = okTest {
    val fillNumberCellJson = Json.obj("value" -> 101)

    val expectedCell = Json.obj("status" -> "ok", "value" -> 101)

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillNumberCellJson)
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, test)
      assertEquals(expectedCell, getResult)
    }
  }

  @Test
  def fillSingleIntegerCell(implicit c: TestContext): Unit = okTest {
    val fillNumberCellJson = Json.obj("value" -> 101)

    val expectedCell = Json.obj("status" -> "ok", "value" -> 101)

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillNumberCellJson)
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, test)
      assertEquals(expectedCell, getResult)
    }

  }

  @Test
  def fillNumberCellWithFloatingNumber(implicit c: TestContext): Unit = okTest {
    for {
      (tableId, columnId, rowId) <- createSimpleTableWithCell("table1", NumericCol("num-column"))

      testInt <- sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> 1234))
      resultInt <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

      testDouble <- sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> 123.123))
      resultDouble <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")
    } yield {
      assertEquals(Json.obj("status" -> "ok", "value" -> 1234), testInt)
      assertEquals(Json.obj("status" -> "ok", "value" -> 1234), resultInt)
      assertEquals(Json.obj("status" -> "ok", "value" -> 123.123), testDouble)
      assertEquals(Json.obj("status" -> "ok", "value" -> 123.123), resultDouble)
    }
  }

  @Test
  def fillNumberCellWithMaxValueNumbers(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, columnId, rowId) <- createSimpleTableWithCell("table1", NumericCol("num-column"))

        testShort <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Short.MaxValue))
        testInt <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testLong <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Long.MaxValue))
        testDouble <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Double.MaxValue))
        testFloat <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
      } yield {
        assertTrue(
          "Short is not equal after retrieving from tableaux",
          Short.MaxValue == testShort.getNumber("value").shortValue()
        )
        assertTrue(
          "Int is not equal after retrieving from tableaux",
          Int.MaxValue == testInt.getNumber("value").intValue()
        )
        assertTrue(
          "Long is not equal after retrieving from tableaux",
          Long.MaxValue == testLong.getNumber("value").longValue()
        )
        assertTrue(
          "Double is not equal after retrieving from tableaux",
          Double.MaxValue == testDouble.getNumber("value").doubleValue()
        )
        assertTrue(
          "Float is not equal after retrieving from tableaux",
          Float.MaxValue == testFloat.getNumber("value").floatValue()
        )
      }
    }
  }

  @Test
  def fillNumberCellWithAlternatingNumberType(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, columnId, rowId) <- createSimpleTableWithCell("table1", NumericCol("num-column"))

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)

        testInt0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Int.MaxValue))
        testFloat0 <-
          sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> Float.MaxValue))
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testInt0)
        _ = assertJSONEquals(Json.obj("status" -> "ok"), testFloat0)
      } yield ()
    }
  }

  @Test
  def fillSingleBooleanCell(implicit c: TestContext): Unit = okTest {
    val fillBooleanCellJson = Json.obj("value" -> true)

    val expectedCell = Json.obj("status" -> "ok", "value" -> true)

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createBooleanColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillBooleanCellJson)
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, test)
      assertEquals(expectedCell, getResult)
    }
  }

  @Test
  def fillDateCell(implicit c: TestContext): Unit = okTest {
    val expectedCell = Json.obj("status" -> "ok", "value" -> "2015-01-01")

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createDateColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST", "/tables/1/columns/1/rows/1", Json.obj("value" -> "2015-01-01"))
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, test)
      assertEquals(expectedCell, getResult)
    }
  }

  @Test
  def fillDateTimeCell(implicit c: TestContext): Unit = okTest {
    val expectedCell = Json.obj("status" -> "ok", "value" -> "2015-01-01T13:37:47.111Z")

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createDateTimeColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST", "/tables/1/columns/1/rows/1", Json.obj("value" -> "2015-01-01T14:37:47.111+01"))
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedCell, test)
      assertEquals(expectedCell, getResult)
    }
  }

  @Test
  def fillTwoDifferentCell(implicit c: TestContext): Unit = okTest {
    val fillNumberCellJson = Json.obj("value" -> 101)
    val fillStringCellJson = Json.obj("value" -> "Test Fill 1")

    val expectedCell1 = Json.obj("status" -> "ok", "value" -> 101)
    val expectedCell2 = Json.obj("status" -> "ok", "value" -> "Test Fill 1")

    for {
      _ <- sendRequest("POST", "/tables", createTableJson)
      _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson)
      _ <- sendRequest("POST", "/tables/1/columns", createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test1 <- sendRequest("POST", "/tables/1/columns/1/rows/1", fillNumberCellJson)
      test2 <- sendRequest("POST", "/tables/1/columns/2/rows/1", fillStringCellJson)
      getResult1 <- sendRequest("GET", "/tables/1/columns/1/rows/1")
      getResult2 <- sendRequest("GET", "/tables/1/columns/2/rows/1")
    } yield {
      assertEquals(expectedCell1, test1)
      assertEquals(expectedCell2, test2)
      assertEquals(expectedCell1, getResult1)
      assertEquals(expectedCell2, getResult2)
    }
  }

  @Test
  def replaceMultiLanguageCell(implicit c: TestContext): Unit = okTest {
    val fillCellJson = Json.obj("value" -> Json.obj("de-DE" -> "Hallo"))

    for {
      (table, _, _) <- createFullTableWithMultilanguageColumns("Test")

      cell <- sendRequest("GET", s"/tables/$table/columns/1/rows/1")

      replacedCell <- sendRequest("PUT", s"/tables/$table/columns/1/rows/1", fillCellJson)
    } yield {
      assertNotSame(cell, replacedCell)
      assertJSONEquals(fillCellJson, replacedCell)
    }
  }

  @Test
  def fillCurrencyCell(implicit c: TestContext): Unit = {
    okTest {
      for {
        (tableId, columnId, rowId) <- createSimpleTableWithCell("table1", CurrencyCol("currency-column"))

        test <- sendRequest("PUT", s"/tables/$tableId/columns/$columnId/rows/$rowId", Json.obj("value" -> 2999.99))
        result <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")
      } yield {
        assertEquals(Json.obj("status" -> "ok", "value" -> 2999.99), test)
        assertEquals(Json.obj("status" -> "ok", "value" -> 2999.99), result)
      }
    }
  }

  @Test
  def fillMultiCountryCurrencyCell(implicit c: TestContext): Unit = {
    okTest {
      val multiCountryCurrencyColumn = MultiCountry(CurrencyCol("currency-column"), Seq("DE", "GB"))

      for {
        (tableId, columnId, rowId) <- createSimpleTableWithCell("table1", multiCountryCurrencyColumn)

        testFloat <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj(
            "value" -> Json.obj(
              "DE" -> 2999.99,
              "GB" -> 3999.99
            )
          )
        )

        resultFloat <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")

        testInt <- sendRequest(
          "POST",
          s"/tables/$tableId/columns/$columnId/rows/$rowId",
          Json.obj(
            "value" -> Json.obj(
              "DE" -> 2999,
              "GB" -> 3999
            )
          )
        )

        resultInt <- sendRequest("GET", s"/tables/$tableId/columns/$columnId/rows/$rowId")
      } yield {
        assertEquals(testInt, resultInt)
        assertEquals(testFloat, resultFloat)

        assertEquals(Json.obj("DE" -> 2999, "GB" -> 3999), resultInt.getJsonObject("value"))
        assertEquals(Json.obj("DE" -> 2999.99, "GB" -> 3999.99), resultFloat.getJsonObject("value"))
      }
    }
  }
}
