package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.json.{Json, JsonObject}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class CreationTest extends TableauxTestBase {

  @Test
  def createTable(): Unit = okTest {
    val jsonObj = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val expectedJson = Json.obj("tableId" -> 1)
    val expectedJson2 = Json.obj("tableId" -> 2)

    for {
      c <- createClient()
      test1 <- sendRequest(c, jsonObj, "/tables")
      test2 <- sendRequest(c, jsonObj, "/tables")
    } yield {
      assertEquals(expectedJson, test1)
      assertEquals(expectedJson2, test2)
    }
  }

  @Test
  def createStringColumn(): Unit = okTest {
    val createJson = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val jsonObj = Json.obj("action" -> "createColumn", "type" -> "String", "tableId" -> 1, "columnName" -> "Test Column 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1)
    
    for {
      c <- createClient()
      t <- sendRequest(c, createJson, "/tables")
      j <- sendRequest(c, jsonObj, "/tables/1/columns")
    } yield {
      assertEquals(expectedJson, j)
    }
  }

  @Test
  def createNumberColumn(): Unit = {
    fail("not implemented")
  }

  @Test
  def createLinkColumn(): Unit = {
    fail("not implemented")
  }


}
