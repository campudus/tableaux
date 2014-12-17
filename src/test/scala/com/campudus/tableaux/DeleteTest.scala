package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

class DeleteTest extends TableauxTestBase {

  val createTableJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
  val delTableJson = Json.obj("action" -> "deleteTable", "tableId" -> 1)
  val createColumnJson = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Nr. 1")
  
  @Test
  def deleteEmptyTable(): Unit = okTest {   
    val expectedJson = Json.obj()

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      test <- sendRequest("DELETE", c, delTableJson, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
  
  @Test
  def deleteWithColumnTable(): Unit = okTest {
    val expectedJson = Json.obj()

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, createTableJson, "/tables")
      _ <- sendRequest("POST", c, createColumnJson, "/tables/1/columns")
      test <- sendRequest("DELETE", c, delTableJson, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def deleteColumn(): Unit = okTest {
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
  
//  @Test
//  def deleteRow(): Unit = {
//    fail("not implemented")
//  }

}