package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class CreationTest extends TableauxTestBase {

  val createTableJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")

  @Test
  def createTable(): Unit = okTest {
    val c = createClient()
    val expectedJson = Json.obj("tableId" -> 1)
    val expectedJson2 = Json.obj("tableId" -> 2)

    for {
      test1 <- sendRequest("POST", c, createTableJson, "/tables")
      test2 <- sendRequest("POST", c, createTableJson, "/tables")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createStringColumn(): Unit = okTest {
    val c = createClient()
    val jsonObj = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Column 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "columnType" -> "text")
    val expectedJson2 = Json.obj("tableId" -> 1, "columnId" -> 2, "columnType" -> "text")

    for {
      t <- sendRequest("POST", c, createTableJson, "/tables")
      test1 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
      test2 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createNumberColumn(): Unit = okTest {
    val c = createClient()
    val jsonObj = Json.obj("action" -> "createColumn", "type" -> "numeric", "tableId" -> 1, "columnName" -> "Test Column 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "columnType" -> "numeric")
    val expectedJson2 = Json.obj("tableId" -> 1, "columnId" -> 2, "columnType" -> "numeric")

    for {
      t <- sendRequest("POST", c, createTableJson, "/tables")
      test1 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
      test2 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createRow(): Unit = okTest {
    val c = createClient()
    val createRowJson = Json.obj("action" -> "createRow", "tableId" -> 1)
    val expectedJson = Json.obj("tableId" -> 1, "rowId" -> 1)
    val expectedJson2 = Json.obj("tableId" -> 1, "rowId" -> 2)

    for {
      t <- sendRequest("POST", c, createTableJson, "/tables")
      test1 <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      test2 <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

}
