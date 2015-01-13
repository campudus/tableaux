package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.http.HttpClientResponse
import org.vertx.testtools.VertxAssert._
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }
import org.vertx.scala.core.json.Json

class TableauxStaticsTest extends TableauxTestBase {

  @Test
  def checkIndexHtml(): Unit = {
    val p1 = readFile()
    val p2 = httpGetIndex()

    for {
      (expected, actual) <- p1.zip(p2)
    } yield {
      assertEquals(expected, actual)
      testComplete()
    }
  }

  @Test
  def setupDatabase(): Unit = for {
    json <- sendRequest("POST", "/reset")
  } yield {
    assertEquals(Json.obj(), json)
    testComplete()
  }

  private def readFile(): Future[String] = {
    val p1 = Promise[String]()
    vertx.fileSystem.readFile("index.html", {
      case Success(buf) =>
        val content = buf.toString()
        p1.success(content)
      case Failure(ex) =>
        logger.error("cannot read file", ex)
        p1.failure(ex)
    }: Try[Buffer] => Unit)
    p1.future
  }

  private def httpGetIndex(): Future[String] = {
    val p = Promise[String]()
    vertx.createHttpClient().setHost("localhost").setPort(8181).get("/", { resp: HttpClientResponse =>
      logger.info("Got a response: " + resp.statusCode())
      resp.bodyHandler { buf => p.success(buf.toString()) }
    }).end()
    p.future
  }

}