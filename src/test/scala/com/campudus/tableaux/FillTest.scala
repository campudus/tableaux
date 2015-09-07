package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class FillTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  @Test
  def fillSingleCellWithNull(): Unit = okTest {
    val fillStringCellJson = Json.obj("value" -> null)

    val expectedJson = Json.obj("status" -> "ok")

    val expectedCell = Json.obj("status" -> "ok", "value" -> null)

    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      fillResult <- sendRequest("POST",  "/tables/1/columns/1/rows/1",  fillStringCellJson)
      cellResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, fillResult)
      assertEquals(expectedCell, cellResult)
    }
  }

  @Test
  def fillSingleStringCell(): Unit = okTest {
    val fillStringCellJson = Json.obj("value" -> "Test Fill 1")

    val expectedJson = Json.obj("status" -> "ok")
    val expectedGet = Json.obj("status" -> "ok", "value" -> "Test Fill 1")

    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST",  "/tables/1/columns/1/rows/1",  fillStringCellJson)
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedGet, getResult)
    }
  }

  @Test
  def fillSingleNumberCell(): Unit = okTest {
    val fillNumberCellJson = Json.obj("value" -> 101)

    val expectedJson = Json.obj("status" -> "ok")
    val expectedGet = Json.obj("status" -> "ok", "value" -> 101)

    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createNumberColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("POST",  "/tables/1/columns/1/rows/1",  fillNumberCellJson)
      getResult <- sendRequest("GET", "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedGet, getResult)
    }
  }

  @Test
  def fillTwoDifferentCell(): Unit = okTest {
    val fillNumberCellJson = Json.obj("value" -> 101)
    val fillStringCellJson = Json.obj("value" -> "Test Fill 1")

    val expectedJson = Json.obj("status" -> "ok")
    val expectedJson2 = Json.obj("status" -> "ok")
    val expectedGet1 = Json.obj("status" -> "ok", "value" -> 101)
    val expectedGet2 = Json.obj("status" -> "ok", "value" -> "Test Fill 1")

    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createNumberColumnJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createStringColumnJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test1 <- sendRequest("POST",  "/tables/1/columns/1/rows/1",  fillNumberCellJson)
      test2 <- sendRequest("POST",  "/tables/1/columns/2/rows/1",  fillStringCellJson)
      getResult1 <- sendRequest("GET", "/tables/1/columns/1/rows/1")
      getResult2 <- sendRequest("GET", "/tables/1/columns/2/rows/1")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
      assertEquals(expectedGet1, getResult1)
      assertEquals(expectedGet2, getResult2)
    }
  }
}
