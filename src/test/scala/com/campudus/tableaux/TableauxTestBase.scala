package com.campudus.tableaux

import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.json.{Json, JsonObject}
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.http._
import scala.concurrent.{Promise, Future}
import scala.util.{Try, Failure, Success}
import com.campudus.tableaux.database.TableStructure

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
trait TableauxTestBase extends TestVerticle {
  val DEFAULT_PORT = 8181
  
  override def asyncBefore(): Future[Unit] = {
    for {
      _ <- deployModule()
      _ <- TableStructure.deinstall(vertx.eventBus)
      _ <- TableStructure.setup(vertx.eventBus)
    } yield ()
  }
  
  def deployModule(): Future[Unit] = {
    val p = Promise[Unit]()
    container.deployModule(System.getProperty("vertx.modulename"), Json.obj(), 1, {
      case Success(id) => p.success()
      case Failure(ex) =>
        logger.error("could not deploy", ex)
        p.failure(ex)
    }: Try[String] => Unit)
    p.future
  }

  def okTest(f: => Future[_]): Unit = {
    (try f catch {
      case ex: Throwable => Future.failed(ex)
    }) onComplete {
      case Success(_) => testComplete()
      case Failure(ex) =>
        logger.error("failed test", ex)
        fail("got exception")
    }
  }
  
  def createClient(): Future[HttpClient] = {
    val p = Promise[HttpClient]()
    p.success(vertx.createHttpClient().setHost("localhost").setPort(DEFAULT_PORT))
    p.future
  }

  def sendRequest(client: HttpClient, jsonObj: JsonObject, path: String): Future[JsonObject] = {
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
