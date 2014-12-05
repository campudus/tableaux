package com.campudus.tableaux

import org.junit.Test
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.http._
import org.vertx.scala.core.json.Json
import scala.concurrent.{ Future, Promise }
import org.vertx.scala.core.json.JsonObject

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
class CreationTest extends TableauxTestBase {

  val DEFAULT_PORT = 8181

  @Test
  def createTable(): Unit = {
    val jsonObj = Json.obj("action" -> "createTable", "tableName" -> "Test Nr. 1")
    val expectedJson = Json.obj("id" -> 1)
    val expectedJson2 = Json.obj("id" -> 2)

    for {
      c <- createClient()
      j <- sendRequest(c, jsonObj, "/tables")
      x <- sendRequest(c, jsonObj, "/tables")
    } yield {
      assertEquals(expectedJson, j)
      assertEquals(expectedJson2, x)
      testComplete()
    }
  }

  @Test
  def createStringColumn(): Unit = {
    val jsonObj = Json.obj("action" -> "createStringColumn", "tableId" -> 1, "columnName" -> "Test Column 1")
    val expectedJson = Json.obj("tableId" -> 1, "columnId" -> 1)
    
    for {
      c <- createClient()
      j <- sendRequest(c, jsonObj, "/columns")
    } yield {
      assertEquals(expectedJson, j)
      testComplete()
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

  private def createClient(): Future[HttpClient] = {
    val p = Promise[HttpClient]()
    p.success(vertx.createHttpClient().setHost("localhost").setPort(DEFAULT_PORT))
    p.future
  }

  private def sendRequest(client: HttpClient, jsonObj: JsonObject, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]
    client.request("POST", path, { resp: HttpClientResponse =>
      logger.info("Got a response: " + resp.statusCode())
      resp.bodyHandler { buf =>
        p.success(Json.fromObjectString(buf.toString))
      }
    }).setChunked(true).write(jsonObj.toString()).end()
    p.future
  }
}
