package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class FillTest extends TableauxTestBase {

  val createTableJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
  val createRowJson = Json.obj("action" -> "createRow", "tableId" -> 1)
  val createNumberColumnJson = Json.obj("action" -> "createColumn", "type" -> "numeric", "tableId" -> 1, "columnName" -> "Test Column 1")
  val createStringColumnJson = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Column 1")

  @Test
  def fillSingleStringCell(): Unit = okTest {
    val fillStringCellJson = Json.obj("action" -> "fillCell", "type" -> "text", "tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> "Test Fill 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> "Test Fill 1")

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      _ <- sendRequest("POST", c, createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      test1 <- sendRequest("POST", c, fillStringCellJson, "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, test1)
    }
  }

  @Test
  def fillSingleNumberCell(): Unit = okTest {
    val fillNumberCellJson = Json.obj("action" -> "fillCell", "type" -> "numeric", "tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> 101)
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> 101)

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      _ <- sendRequest("POST", c, createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      test1 <- sendRequest("POST", c, fillNumberCellJson, "/tables/1/columns/1/rows/1")
    } yield {
      assertEquals(expectedJson, test1)
    }
  }

  @Test
  def fillTwoDifferentCell(): Unit = okTest {
    val fillNumberCellJson = Json.obj("action" -> "fillCell", "type" -> "numeric", "tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> 101)
    val fillStringCellJson = Json.obj("action" -> "fillCell", "type" -> "text", "tableId" -> 1, "columnId" -> 2, "rowId" -> 1, "value" -> "Test Fill 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> 101)
    val expectedJson2 = Json.obj("tableId" -> 1, "columnId" -> 2, "rowId" -> 1, "value" -> "Test Fill 1")

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      _ <- sendRequest("POST", c, createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", c, createStringColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      test1 <- sendRequest("POST", c, fillNumberCellJson, "/tables/1/columns/1/rows/1")
      test2 <- sendRequest("POST", c, fillStringCellJson, "/tables/1/columns/2/rows/1")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def fillCompleteRow(): Unit = {
    fail("not implemented")
  }

}
