package com.campudus.tableaux

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class ChangeTest extends TableauxTestBase {

  val expectedJson = Json.obj("status" -> "ok")

  @Test
  def changeTableName(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("name" -> "New testname")
    val expectedTableJson = Json.obj("status" -> "ok", "id" -> 1, "name" -> "New testname", "hidden" -> false)

    for {
      _ <- setupDefaultTable()
      test1 <- sendRequest("POST", "/tables/1", postJson)
      test2 <- sendRequest("GET", "/tables/1")
    } yield {
      assertEquals(expectedTableJson, test1)
      assertEquals(expectedTableJson, test2)
    }
  }

  @Test
  def changeColumnName(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("name" -> "New testname")
    val expectedString = "New testname"

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("POST", "/tables/1/columns/1", postJson)
      test2 <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedString, test2.getString("name"))
    }
  }

  @Test
  def changeColumnOrdering(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("ordering" -> 5)
    val expectedOrdering = 5

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("POST", "/tables/1/columns/1", postJson)
      test2 <- sendRequest("GET", "/tables/1/columns/1")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedOrdering, test2.getInteger("ordering"))
    }
  }

  @Test
  def changeColumnKind(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("kind" -> "text")
    val expectedKind = "text"

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("POST", "/tables/1/columns/2", postJson)
      test2 <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedKind, test2.getString("kind"))
    }
  }

  @Test
  def changeColumnKindWhichShouldFail(implicit c: TestContext): Unit = okTest {
    val kindTextJson = Json.obj("kind" -> "text")
    val kindNumericJson = Json.obj("kind" -> "numeric")

    val failed = Json.obj("failed" -> "failed")

    for {
      _ <- setupDefaultTable()

      _ <- sendRequest("POST", "/tables/1/rows", Json.obj("rows" ->
        Json.obj("values" ->
          Json.arr("Test", 5)
        )
      ))

      // change numeric column to text column
      changeToText <- sendRequest("POST", "/tables/1/columns/2", kindTextJson)

      // change text column to numeric column, which should fail
      failedChangeToNumeric <- sendRequest("POST", "/tables/1/columns/1", kindNumericJson)
        .recoverWith({ case _ => Future.successful(failed) })

      columns <- sendRequest("GET", "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, changeToText)

      assertEquals(failed, failedChangeToNumeric)

      assertEquals("text", columns.getJsonArray("columns").getJsonObject(0).getString("kind"))
      assertEquals("text", columns.getJsonArray("columns").getJsonObject(1).getString("kind"))
    }
  }

  @Test
  def changeColumn(implicit c: TestContext): Unit = okTest {
    val postJson = Json.obj("name" -> "New testname", "ordering" -> 5, "kind" -> "text")
    val expectedJson2 = Json.obj("status" -> "ok", "id" -> 2, "name" -> "New testname", "kind" -> "text", "ordering" -> 5, "multilanguage" -> false, "identifier" -> false)

    for {
      _ <- setupDefaultTable()
      test <- sendRequest("POST", "/tables/1/columns/2", postJson)
      test2 <- sendRequest("GET", "/tables/1/columns/2")
    } yield {
      assertEquals(expectedJson, test)
      assertEquals(expectedJson2, test2)
    }
  }
}