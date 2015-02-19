package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

class DeleteTest extends TableauxTestBase {

  val createTableJson = Json.obj("tableName" -> "Test Nr. 1")
  val createColumnJson = Json.obj("type" -> Json.arr("text"), "columnName" -> Json.arr("Test Column 1"))

  @Test
  def deleteEmptyTable(): Unit = okTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(Json.obj(), test)
    }
  }

  @Test
  def deleteWithColumnTable(): Unit = okTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createColumnJson, "/tables/1/columns")
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(Json.obj(), test)
    }
  }

  @Test
  def deleteColumn(): Unit = okTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequestWithJson("POST", createColumnJson, "/tables/1/columns")
      test <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield {
      assertEquals(Json.obj(), test)
    }
  }

  @Test
  def deleteRow(): Unit = okTest {
    for {
      _ <- sendRequestWithJson("POST", createTableJson, "/tables")
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield {
      assertEquals(Json.obj(), test)
    }
  }

}