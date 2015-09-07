package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.Json

class DeleteTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")
  val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
  val expectedJson = Json.obj("status" -> "ok")

  @Test
  def deleteEmptyTable(): Unit = okTest {
    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def deleteTableWithColumn(): Unit = okTest {
    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createStringColumnJson)
      test <- sendRequest("DELETE", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def deleteColumn(): Unit = okTest {
    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST",  "/tables/1/columns",  createStringColumnJson)
      test <- sendRequest("DELETE", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

  @Test
  def deleteRow(): Unit = okTest {
    for {
      _ <- sendRequest("POST",  "/tables",  createTableJson)
      _ <- sendRequest("POST", "/tables/1/rows")
      test <- sendRequest("DELETE", "/tables/1/rows/1")
    } yield {
      assertEquals(expectedJson, test)
    }
  }

}