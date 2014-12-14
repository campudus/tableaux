package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class CreationTest extends TableauxTestBase {

  @Test
  def createTable(): Unit = okTest {
    val jsonObj = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val expectedJson = Json.obj("tableId" -> 1)
    val expectedJson2 = Json.obj("tableId" -> 2)

    for {
      c <- createClient()
      test1 <- sendRequest("POST", c, jsonObj, "/tables")
      test2 <- sendRequest("POST", c, jsonObj, "/tables")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createStringColumn(): Unit = okTest {
    val createJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val jsonObj = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Column 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "columnType" -> "text")
    val expectedJson2 = Json.obj("tableId" -> 1, "columnId" -> 2, "columnType" -> "text")

    for {
      c <- createClient()
      t <- sendRequest("POST", c, createJson, "/tables")
      test1 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
      test2 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createNumberColumn(): Unit = okTest {
    val createJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val jsonObj = Json.obj("action" -> "createColumn", "type" -> "numeric", "tableId" -> 1, "columnName" -> "Test Column 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "columnType" -> "numeric")
    val expectedJson2 = Json.obj("tableId" -> 1, "columnId" -> 2, "columnType" -> "numeric")

    for {
      c <- createClient()
      t <- sendRequest("POST", c, createJson, "/tables")
      test1 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
      test2 <- sendRequest("POST", c, jsonObj, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createLinkColumn(): Unit = {
    fail("not implemented")
  }

}
