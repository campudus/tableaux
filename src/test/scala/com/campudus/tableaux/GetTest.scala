package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.http.HttpClient

class GetTest extends TableauxTestBase {

  val postTable = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
  val getTable = Json.obj("action" -> "getTable", "tableId" -> 1)
  val postTextCol = Json.obj("action" -> "createColumn", "type" -> "text", "tableId" -> 1, "columnName" -> "Test Column 1")
  val postNumCol = Json.obj("action" -> "createColumn", "type" -> "numeric", "tableId" -> 1, "columnName" -> "Test Column 2")
  val getColumn = Json.obj("action" -> "getColumn", "tableId" -> 1, "columnId" -> 1)
  val createRowJson = Json.obj("action" -> "createRow", "tableId" -> 1)

  @Test
  def getEmptyTable(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr(), "rows" -> Json.arr())
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
    val expectedJson = Json.obj(
      "tableId" -> 1,
      "tableName" -> "Test Nr. 1",
      "cols" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1"),
        Json.obj("id" -> 2, "name" -> "Test Column 2")),
      "rows" -> Json.arr())

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
  def getWithColumnAndRowTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "tableId" -> 1,
      "tableName" -> "Test Nr. 1",
      "cols" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1"),
        Json.obj("id" -> 2, "name" -> "Test Column 2")),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "c1" -> null, "c2" -> null)))

    for {
      c <- createClient()
      _ <- sendRequest("POST", c, postTable, "/tables")
      _ <- sendRequest("POST", c, postTextCol, "/tables/1/columns")
      _ <- sendRequest("POST", c, postNumCol, "/tables/1/columns")
      _ <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      test <- sendRequest("GET", c, getTable, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getFullTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "tableId" -> 1,
      "tableName" -> "Test Nr. 1",
      "cols" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1"),
        Json.obj("id" -> 2, "name" -> "Test Column 2")),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "c1" -> "Test Fill 1", "c2" -> 1),
        Json.obj("id" -> 2, "c1" -> "Test Fill 2", "c2" -> 2)))

    for {
      c <- setupTables()
      test <- sendRequest("GET", c, getTable, "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  //  @Test
  //  def getAllTables(): Unit = {
  //    fail("not implemented")
  //  }

  @Test
  def getStringColumn(): Unit = okTest {
    val expectedJson = Json.obj("columnId" -> 1, "columnName" -> "Test Column 1", "type" -> "text")

    for {
      c <- setupTables()
      test <- sendRequest("GET", c, getColumn, "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("columnId" -> 2, "columnName" -> "Test Column 2", "type" -> "numeric")

    for {
      c <- setupTables()
      test <- sendRequest("GET", c, getColumn, "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  private def setupTables(): Future[HttpClient] = {
    val fillStringCellJson = Json.obj("action" -> "fillCell", "type" -> "text", "tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> "Test Fill 1")
    val fillStringCellJson2 = Json.obj("action" -> "fillCell", "type" -> "text", "tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> "Test Fill 2")
    val fillNumberCellJson = Json.obj("action" -> "fillCell", "type" -> "numeric", "tableId" -> 1, "columnId" -> 2, "rowId" -> 1, "value" -> 1)
    val fillNumberCellJson2 = Json.obj("action" -> "fillCell", "type" -> "numeric", "tableId" -> 1, "columnId" -> 2, "rowId" -> 2, "value" -> 2)
    for {
      c <- createClient()
      _ <- sendRequest("POST", c, postTable, "/tables")
      _ <- sendRequest("POST", c, postTextCol, "/tables/1/columns")
      _ <- sendRequest("POST", c, postNumCol, "/tables/1/columns")
      _ <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      _ <- sendRequest("POST", c, createRowJson, "/tables/1/rows")
      _ <- sendRequest("POST", c, fillStringCellJson, "/tables/1/columns/1/rows/1")
      _ <- sendRequest("POST", c, fillStringCellJson2, "/tables/1/columns/1/rows/2")
      _ <- sendRequest("POST", c, fillNumberCellJson, "/tables/1/columns/2/rows/1")
      _ <- sendRequest("POST", c, fillNumberCellJson2, "/tables/1/columns/2/rows/2")
    } yield c
  }
  
}