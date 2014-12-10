package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json
import scala.concurrent.Future

class GetTest extends TableauxTestBase {

  @Test
  def getEmptyTable(): Unit = okTest {
    val jsonObj = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val getJson = Json.obj("action" -> "getTable", "tableId" -> 1)
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr())

    for {
      c <- createClient()
      x <- sendRequest("POST", c, jsonObj, "/tables")
      test <- sendRequest("GET", c, getJson, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
  
  @Test
  def getWithColumnTable(): Unit = okTest {
    val jsonObj = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val postCol = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Column 1")
    val getJson = Json.obj("action" -> "getTable", "tableId" -> 1)
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr(Json.obj("columnId" -> 1, "columnName" -> "Test Column 1")))

    for {
      c <- createClient()
      x <- sendRequest("POST", c, jsonObj, "/tables")
      i <- sendRequest("POST", c, postCol, "/tables/1/columns")
      test <- sendRequest("GET", c, getJson, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
  
  @Test
  def getColumn(): Unit = {
    fail("not implemented")
  }
  
}