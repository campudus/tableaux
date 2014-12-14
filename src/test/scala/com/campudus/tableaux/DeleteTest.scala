package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

class DeleteTest extends TableauxTestBase {

  @Test
  def deleteEmptyTable(): Unit = okTest {
    val jsonObj = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val delJson = Json.obj("action" -> "deleteTable", "tableId" -> 1)
    val expectedJson = Json.obj()

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, jsonObj, "/tables")
      test <- sendRequest("DELETE", c, delJson, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
  
  @Test
  def deleteWithColumnTable(): Unit = okTest {
    val createTableJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val createColumnJson = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Nr. 1")
    val delJson = Json.obj("action" -> "deleteTable", "tableId" -> 1)
    val expectedJson = Json.obj()

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      _ <- sendRequest("POST", c, createColumnJson, "/tables/1/columns")
      test <- sendRequest("DELETE", c, delJson, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def deleteColumn(): Unit = okTest {
    val createTableJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val createColumnJson = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Nr. 1")
    val delJson = Json.obj("action" -> "deleteColumn", "tableId" -> 1, "columnId" -> 1)
    val expectedJson = Json.obj()

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      _ <- sendRequest("POST", c, createColumnJson, "/tables/1/columns")
      test <- sendRequest("DELETE", c, delJson, "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
  
  @Test
  def deleteRow(): Unit = {
    fail("not implemented")
  }

}