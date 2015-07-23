package com.campudus.tableaux


import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.http._
import org.vertx.scala.core.json._
import org.vertx.scala.testtools.TestVerticle
import org.vertx.testtools.VertxAssert._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

case class TestCustomException(message: String, id: String, statusCode: Int) extends Throwable

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
trait TableauxTestBase extends TestVerticle with TestConfig {

  override val verticle = this

  override def asyncBefore(): Future[Unit] = {
    val dbConnection = DatabaseConnection(tableauxConfig)
    val system = SystemModel(dbConnection)

    for {
      _ <- deployModule(config)
      _ <- system.deinstall()
      _ <- system.setup()
    } yield ()
  }

  def deployModule(config: JsonObject): Future[Unit] = {
    val p = Promise[Unit]()
    container.deployModule(System.getProperty("vertx.modulename"), config, 1, {
      case Success(id) => p.success(())
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

  def exceptionTest(id: String)(f: => Future[_]): Unit = {
    f onComplete {
      case Success(_) =>
        logger.error("test should fail")
        fail("got no exception")
      case Failure(ex: TestCustomException) =>
        assertEquals(id, ex.id)
        testComplete()
      case Failure(ex) =>
        logger.error(s"test should fail with RouterException")
        fail("got wrong exception")
    }
  }

  def createClient(): HttpClient = {
    vertx.createHttpClient().setHost("localhost").setPort(port)
  }

  def sendRequest(method: String, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p).end()
    p.future
  }

  def sendRequest(method: String, path: String, jsonObj: JsonObject): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p).setChunked(true).write(jsonObj.encode()).end()
    p.future
  }

  @Deprecated
  def sendRequestWithJson(method: String, jsonObj: JsonObject, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]()
    httpJsonRequest(method, path, p).setChunked(true).write(jsonObj.encode()).end()
    p.future
  }

  def jsonResponse(p: Promise[JsonObject]): HttpClientResponse => Unit = { resp: HttpClientResponse =>
    resp.bodyHandler { buf =>
      if (resp.statusCode() != 200) {
        p.failure(TestCustomException(buf.toString(), resp.statusMessage(), resp.statusCode()))
      } else {
        try {
          p.success(Json.fromObjectString(buf.toString()))
        } catch {
          case ex: Exception => p.failure(ex)
        }
      }
    }
  }

  private def httpJsonRequest(method: String, path: String, p: Promise[JsonObject]): HttpClientRequest = {
    httpRequest(method, path, jsonResponse(p))
  }

  def httpRequest(method: String, path: String, responseHandler: HttpClientResponse => Unit): HttpClientRequest = {
    val client = createClient()
      .exceptionHandler({ x => fail("Vertx HttpClient failed: " + x.getMessage) })

    client.request(method, path, responseHandler)
  }

  def setupDefaultTable(name: String = "Test Table 1", tableNum: Int = 1): Future[Long] = {
    val postTable = Json.obj("name" -> name)
    val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
    val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
    val fillStringCellJson = Json.obj("value" -> s"table${tableNum}row1")
    val fillStringCellJson2 = Json.obj("value" -> s"table${tableNum}row2")
    val fillNumberCellJson = Json.obj("value" -> 1)
    val fillNumberCellJson2 = Json.obj("value" -> 2)

    for {
      tableId <- sendRequestWithJson("POST", postTable, "/tables") map { js => js.getLong("id") }
      _ <- sendRequestWithJson("POST", createStringColumnJson, s"/tables/$tableId/columns")
      _ <- sendRequestWithJson("POST", createNumberColumnJson, s"/tables/$tableId/columns")
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequestWithJson("POST", fillStringCellJson, s"/tables/$tableId/columns/1/rows/1")
      _ <- sendRequestWithJson("POST", fillStringCellJson2, s"/tables/$tableId/columns/1/rows/2")
      _ <- sendRequestWithJson("POST", fillNumberCellJson, s"/tables/$tableId/columns/2/rows/1")
      _ <- sendRequestWithJson("POST", fillNumberCellJson2, s"/tables/$tableId/columns/2/rows/2")
    } yield tableId
  }
}