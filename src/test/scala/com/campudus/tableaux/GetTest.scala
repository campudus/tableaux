package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.http.HttpClient

class GetTest extends TableauxTestBase {

  val postTable = Json.obj("tableName" -> "Test Nr. 1")
  val postTextCol = Json.obj("type" -> "text", "columnName" -> "Test Column 1")
  val postNumCol = Json.obj("type" -> "numeric", "columnName" -> "Test Column 2")

  @Test
  def getEmptyTable(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "tableName" -> "Test Nr. 1", "cols" -> Json.arr(), "rows" -> Json.arr())

    for {
      _ <- sendRequestWithJson("POST", postTable, "/tables")
      test <- sendRequest("GET", "/tables/1")
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
      _ <- sendRequestWithJson("POST", postTable, "/tables")
      _ <- sendRequestWithJson("POST", postTextCol, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", postNumCol, "/tables/1/columns")
      test <- sendRequest("GET", "/tables/1")
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
      _ <- sendRequestWithJson("POST", postTable, "/tables")
      _ <- sendRequestWithJson("POST", postTextCol, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", postNumCol, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("GET", "/tables/1")
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
      test <- sendRequest("GET", "/tables/1")
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
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1, "columnName" -> "Test Column 1", "type" -> "text")

    for {
      c <- setupTables()
      test <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 2, "columnName" -> "Test Column 2", "type" -> "numeric")

    for {
      c <- setupTables()
      test <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getRow(): Unit = okTest {
    val expectedJson = Json.obj("tableId" -> 1, "rowId" -> 1, "values" -> Json.arr("Test Fill 1", 1))

    for {
      c <- setupTables()
      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  private def setupTables(): Future[Unit] = {
    val fillStringCellJson = Json.obj("type" -> "text", "value" -> "Test Fill 1")
    val fillStringCellJson2 = Json.obj("type" -> "text", "value" -> "Test Fill 2")
    val fillNumberCellJson = Json.obj("type" -> "numeric", "value" -> 1)
    val fillNumberCellJson2 = Json.obj("type" -> "numeric", "value" -> 2)

    for {
      _ <- sendRequestWithJson("POST", postTable, "/tables")
      _ <- sendRequestWithJson("POST", postTextCol, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", postNumCol, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      _ <- sendRequest("POST", "/tables/1/rows")
      _ <- sendRequestWithJson("POST", fillStringCellJson, "/tables/1/columns/1/rows/1")
      _ <- sendRequestWithJson("POST", fillStringCellJson2, "/tables/1/columns/1/rows/2")
      _ <- sendRequestWithJson("POST", fillNumberCellJson, "/tables/1/columns/2/rows/1")
      _ <- sendRequestWithJson("POST", fillNumberCellJson2, "/tables/1/columns/2/rows/2")
    } yield ()
  }

}