package com.campudus.tableaux

import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.json.{ Json, JsonObject }
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._
import org.vertx.scala.core.http._
import scala.concurrent.{ Promise, Future }
import scala.util.{ Try, Failure, Success }
import com.campudus.tableaux.database.SystemStructure
import com.campudus.tableaux.database.DatabaseConnection

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
trait TableauxTestBase extends TestVerticle {
  val DEFAULT_PORT = 8181

  override def asyncBefore(): Future[Unit] = {
    val transaction = new DatabaseConnection(this)
    val system = new SystemStructure(transaction)
    for {
      _ <- deployModule()
      _ <- system.deinstall()
      _ <- system.setup()
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

  def sendRequest(method: String, client: HttpClient, jsonObj: JsonObject, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]
    client.request(method, path, { resp: HttpClientResponse =>
      logger.info("Got a response: " + resp.statusCode())
      resp.bodyHandler { buf =>
        p.success(Json.fromObjectString(buf.toString))
      }
    }).setChunked(true).write(jsonObj.encode()).end()
    p.future
  }
}
