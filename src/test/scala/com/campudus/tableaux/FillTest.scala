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
  def fillSingleStringCell(): Unit = okTest {
    val fillStringCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> "Test Fill 1")))
    val expectedJson = Json.obj("status" -> "ok")

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequestWithJson("POST", fillStringCellJson, "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def fillSingleNumberCell(): Unit = okTest {
    val fillNumberCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> 101)))
    val expectedJson = Json.obj("status" -> "ok")

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequestWithJson("POST", fillNumberCellJson, "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def fillTwoDifferentCell(): Unit = okTest {
    val fillNumberCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> 101)))
    val fillStringCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> "Test Fill 1")))
    val expectedJson = Json.obj("status" -> "ok")
    val expectedJson2 = Json.obj("status" -> "ok")

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      test1 <- sendRequestWithJson("POST", fillNumberCellJson, "/tables/1/columns/1/rows/1")
      test2 <- sendRequestWithJson("POST", fillStringCellJson, "/tables/1/columns/2/rows/1")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  //  @Test
  //  def fillCompleteRow(): Unit = {
  //    fail("not implemented")
  //  }

}
