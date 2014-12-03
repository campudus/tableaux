package com.campudus.tableaux

import org.junit.Test
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.http.HttpClientResponse
import org.vertx.testtools.VertxAssert._

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

class TableauxStaticsTest extends TableauxTestBase {

  @Test
  def checkIndexHtml(): Unit = {
    val p1, p2 = Promise[String]()
    vertx.fileSystem.readFile("index.html", {
      case Success(buf) =>
        val content = buf.toString()
        p1.success(content)
      case Failure(ex) =>
        logger.error("cannot read file", ex)
        p2.failure(ex)
    }: Try[Buffer] => Unit)

    vertx.createHttpClient().setHost("localhost").setPort(8181).get("/", { resp: HttpClientResponse =>
      logger.info("Got a response: " + resp.statusCode())
      resp.bodyHandler { buf => p2.success(buf.toString())}
    }).end()

    p1.future.zip(p2.future).map {
      case (expected, actual) =>
        assertEquals(expected, actual)
        testComplete()
    }
  }

}