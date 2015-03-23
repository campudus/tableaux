package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.json.Json
import org.vertx.testtools.VertxAssert._
import scala.concurrent.Future
import org.vertx.scala.core.http.HttpClient

class GetTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Table 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))

  @Test
  def getEmptyTable(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "tableId" -> 1, "tableName" -> "Test Table 1", "columns" -> Json.arr(), "rows" -> Json.arr())

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getWithColumnTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "tableId" -> 1,
      "tableName" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2)),
      "rows" -> Json.arr())

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getWithColumnAndRowTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "tableId" -> 1,
      "tableName" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2)),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr(null, null))))

    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createStringColumnJson, "/tables/1/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, "/tables/1/columns")
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getFullTable(): Unit = okTest {
    val expectedJson = Json.obj(
      "status" -> "ok",
      "tableId" -> 1,
      "tableName" -> "Test Table 1",
      "columns" -> Json.arr(
        Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1),
        Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2)),
      "rows" -> Json.arr(
        Json.obj("id" -> 1, "values" -> Json.arr("Test Fill 1", 1)),
        Json.obj("id" -> 2, "values" -> Json.arr("Test Fill 2", 2))))

    for {
      _ <- setupDefaultTable()
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
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 1, "name" -> "Test Column 1", "kind" -> "text", "ordering" -> 1)))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getNumberColumn(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "name" -> "Test Column 2", "kind" -> "numeric", "ordering" -> 2)))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getRow(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("id" -> 1, "values" -> Json.arr("Test Fill 1", 1))))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def getCell(): Unit = okTest {
    val expectedJson = Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("value" -> "Test Fill 1")))
    val expectedJson2 = Json.obj("status" -> "ok", "rows" -> Json.arr(Json.obj("value" -> 1)))

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("GET", "/tables/1/columns/1/rows/1")
      test2 <- sendRequest("GET", "/tables/1/columns/2/rows/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedJson2, test2)
    }
  }
}