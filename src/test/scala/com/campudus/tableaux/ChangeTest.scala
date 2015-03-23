package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.json.{ Json, JsonObject }

class ChangeTest extends TableauxTestBase {

  val expectedJson = Json.obj("status" -> "ok")

  @Test
  def changeTableName(): Unit = okTest {
    val postJson = Json.obj("tableName" -> "New testname")
    val expectedString = "New testname"

    for {
      _ <- setupDefaultTable()
      test <- sendRequestWithJson("POST", postJson, "/tables/1")
      test2 <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedString, test2.getString("tableName"))
    }
  }

  @Test
  def changeColumnName(): Unit = okTest {
    val postJson = Json.obj("name" -> "New testname")
    val expectedString = "New testname"

    for {
      _ <- setupDefaultTable()
      test <- sendRequestWithJson("POST", postJson, "/tables/1/columns/1")
      test2 <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedString, test2.getArray("columns").get[JsonObject](0).getString("name"))
    }
  }

  @Test
  def changeColumnOrdering(): Unit = okTest {
    val postJson = Json.obj("ordering" -> 5)
    val expectedOrdering = 5

    for {
      _ <- setupDefaultTable()
      test <- sendRequestWithJson("POST", postJson, "/tables/1/columns/1")
      test2 <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedOrdering, test2.getArray("columns").get[JsonObject](0).getNumber("ordering"))
    }
  }

  @Test
  def changeColumnKind(): Unit = okTest {
    val postJson = Json.obj("kind" -> "text")
    val expectedKind = "text"

    for {
      _ <- setupDefaultTable()
      test <- sendRequestWithJson("POST", postJson, "/tables/1/columns/2")
      test2 <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedKind, test2.getArray("columns").get[JsonObject](0).getString("kind"))
    }
  }

  @Test
  def changeColumn(): Unit = okTest {
    val postJson = Json.obj("name" -> "New testname", "ordering" -> 5, "kind" -> "text")
    val expectedJson2 = Json.obj("status" -> "ok", "columns" -> Json.arr(Json.obj("id" -> 2, "name" -> "New testname", "kind" -> "text", "ordering" -> 5)))

    for {
      _ <- setupDefaultTable()
      test <- sendRequestWithJson("POST", postJson, "/tables/1/columns/2")
      test2 <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedJson2, test2)
    }
  }
}