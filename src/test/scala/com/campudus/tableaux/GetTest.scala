package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._

class GetTest extends TableauxTestBase {

  val postTable = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
  val getTable = Json.obj("action" -> "getTable", "tableId" -> 1)
  val postTextCol = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Column 1")
  val postNumCol = Json.obj("action" -> "createColumn", "type" -> "numeric", "tableId" -> 1, "columnName" -> "Test Column 2")
  val getColumn = Json.obj("action" -> "getColumn", "tableId" -> 1, "columnId" -> 1)

  @Test
  def getEmptyTable(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr())
    for {
      c <- createClient()
      _ <- sendRequest("POST", c, postTable, "/tables")
      test <- sendRequest("GET", c, getTable, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getWithColumnTable(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr(
      Json.obj("columnId" -> 1, "columnName" -> "Test Column 1"),
      Json.obj("columnId" -> 2, "columnName" -> "Test Column 2")))

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, postTable, "/tables")
      _ <- sendRequest("POST", c, postTextCol, "/tables/1/columns")
      _ <- sendRequest("POST", c, postNumCol, "/tables/1/columns")
      test <- sendRequest("GET", c, getTable, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getAllTables(): Unit = {
    fail("not implemented")
  }

  @Test
  def getStringColumn(): Unit = okTest {
    val expectedJson = Json.obj("columnId" -> 1, "columnName" -> "Test Column 1", "type" -> "text")

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, postTable, "/tables")
      _ <- sendRequest("POST", c, postTextCol, "/tables/1/columns")
      test <- sendRequest("GET", c, getColumn, "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("columnId" -> 1, "columnName" -> "Test Column 2", "type" -> "numeric")

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, postTable, "/tables")
      _ <- sendRequest("POST", c, postNumCol, "/tables/1/columns")
      test <- sendRequest("GET", c, getColumn, "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }
  
}