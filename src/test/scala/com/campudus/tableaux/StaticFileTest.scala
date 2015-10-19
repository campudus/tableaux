package com.campudus.tableaux

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientResponse
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.FunctionConverters._
import org.junit.Test
import org.junit.runner.RunWith

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

@RunWith(classOf[VertxUnitRunner])
class StaticFileTest extends TableauxTestBase {

  @Test
  def checkIndexHtml(implicit c: TestContext): Unit = okTest {
    val p1 = readFile()
    val p2 = httpGetIndex()

    for {
      (expected, actual) <- p1.zip(p2)
    } yield {
      assertEquals(expected, actual)
    }
  }

  private def readFile(): Future[String] = {
    val p1 = Promise[String]()
    vertx.fileSystem.readFile("index.html", {
      case Success(buf) =>
        val content = buf.toString(): String
        p1.success(content)
      case Failure(ex) =>
        logger.error("cannot read file", ex)
        p1.failure(ex)
    }: Try[Buffer] => Unit)
    p1.future
  }

  private def httpGetIndex(): Future[String] = {
    val p = Promise[String]()

    def responseHandler(resp: HttpClientResponse): Unit = {
      logger.info("Got a response: " + resp.statusCode())
      resp.bodyHandler({ buf: Buffer => p.success(buf.toString); return; })
    }

    httpRequest("GET", "/", responseHandler, { x => p.failure(x) }).end()

    p.future
  }
}