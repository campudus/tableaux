package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class FillTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Nr. 1")
  val createNumberColumnJson = Json.obj("type" -> "numeric", "columnName" -> "Test Column 1")
  val createStringColumnJson = Json.obj("type" -> "text", "columnName" -> "Test Column 1")

  @Test
  def fillSingleStringCell(): Unit = okTest {
    val fillStringCellJson = Json.obj("type" -> "text", "value" -> "Test Fill 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> "Test Fill 1")

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
    val fillNumberCellJson = Json.obj("type" -> "numeric", "value" -> 101)
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> 101)

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
    val fillNumberCellJson = Json.obj("type" -> "numeric", "value" -> 101)
    val fillStringCellJson = Json.obj("type" -> "text", "value" -> "Test Fill 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> 101)
    val expectedJson2 = Json.obj("tableId" -> 1, "columnId" -> 2, "rowId" -> 1, "value" -> "Test Fill 1")

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
