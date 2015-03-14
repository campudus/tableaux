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
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.router.RouterException

case class TestCustomException(message: String, id: String, statusCode: Int) extends Throwable

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

  def createClient(): HttpClient = vertx.createHttpClient().setHost("localhost").setPort(DEFAULT_PORT)

  def sendRequest(method: String, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]
    httpClientRequest(method, path, p).end()
    p.future
  }

  def sendRequestWithJson(method: String, jsonObj: JsonObject, path: String): Future[JsonObject] = {
    val p = Promise[JsonObject]
    httpClientRequest(method, path, p).setChunked(true).write(jsonObj.encode()).end()
    p.future
  }

  private def httpClientRequest(method: String, path: String, p: Promise[JsonObject]): HttpClientRequest = createClient().request(method, path, { resp: HttpClientResponse =>
    logger.info("Got a response: " + resp.statusCode())

    resp.bodyHandler { buf =>
      logger.info("response: " + buf.toString())
      if (resp.statusCode() != 200) {
        p.failure(TestCustomException(buf.toString(), resp.statusMessage(), resp.statusCode()))
      } else {
        try {
          p.success(Json.fromObjectString(buf.toString))
        } catch {
          case ex: Exception => p.failure(ex)
        }
      }
    }
  })

  def setupDefaultTable(name: String = "Test Table 1"): Future[Long] = {
    val postTable = Json.obj("tableName" -> name)
    val createStringColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1")))
    val createNumberColumnJson = Json.obj("columns" -> Json.arr(Json.obj("kind" -> "numeric", "name" -> "Test Column 2")))
    val fillStringCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> "Test Fill 1")))
    val fillStringCellJson2 = Json.obj("cells" -> Json.arr(Json.obj("value" -> "Test Fill 2")))
    val fillNumberCellJson = Json.obj("cells" -> Json.arr(Json.obj("value" -> 1)))
    val fillNumberCellJson2 = Json.obj("cells" -> Json.arr(Json.obj("value" -> 2)))

    for {
      tableId <- sendRequestWithJson("POST", postTable, "/tables") map { js => js.getLong("tableId") }
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
