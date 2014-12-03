package com.campudus.tableaux

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.junit.Test
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.http.HttpClientResponse
import org.vertx.scala.core.json.Json
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._

class TableauxStaticsTest extends TestVerticle {

  override def asyncBefore(): Future[Unit] = {
    val p = Promise[Unit]
    container.deployModule(System.getProperty("vertx.modulename"), Json.obj(), 1, {
      case Success(id) => p.success()
      case Failure(ex) =>
        logger.error("could not deploy", ex)
        p.failure(ex)
    }: Try[String] => Unit)
    p.future
  }

  @Test
  def getIndexHtml(): Unit = {
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
      resp.bodyHandler { buf => p2.success(buf.toString()) }
    }).end()

    p1.future.zip(p2.future).map {
      case (expected, actual) =>
        assertEquals(expected, actual)
        testComplete()
    }
  }

}