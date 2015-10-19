package com.campudus.tableaux


import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.{HttpClient, HttpClientRequest, HttpClientResponse, HttpMethod}
import io.vertx.core.{AsyncResult, Handler, Vertx}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.{SQLConnection, VertxExecutionContext}
import org.junit.runner.RunWith
import org.junit.{After, Before}
import org.vertx.scala.core.json._

import scala.concurrent.{Future, Promise}
import scala.util.{Try, Failure, Success}

case class TestCustomException(message: String, id: String, statusCode: Int) extends Throwable

trait TestAssertionHelper {
  def assertEquals(message: String, excepted: Any, actual: Any)(implicit c: TestContext): TestContext = {
    c.assertEquals(excepted, actual, message)
  }

  def assertEquals(excepted: Any, actual: Any)(implicit c: TestContext): TestContext = {
    c.assertEquals(excepted, actual)
  }

  def assertNull(excepted: Any)(implicit c: TestContext): TestContext = {
    c.assertNotNull(excepted)
  }

  def assertTrue(message: String, condition: Boolean)(implicit c: TestContext): TestContext = {
    c.assertTrue(condition, message)
  }

  def assertTrue(condition: Boolean)(implicit c: TestContext): TestContext = {
    c.assertTrue(condition)
  }

  def assertNotSame(first: Any, second: Any)(implicit c: TestContext): TestContext = {
    c.assertNotEquals(first, second)
  }
}

@RunWith(classOf[VertxUnitRunner])
trait TableauxTestBase extends TestConfig with LazyLogging with TestAssertionHelper with JsonCompatible with VertxExecutionContext {

  override val verticle = new Starter

  val vertx: Vertx = Vertx.vertx()
  private var deploymentId: String = ""

  @Before
  def before(context: TestContext) {
    val async = context.async()

    vertx.deployVerticle(verticle, {
      case Success(id) =>
        logger.info(s"Verticle deployed with ID $id")
        this.deploymentId = id

        val sqlConnection = SQLConnection(vertx, databaseConfig)
        val dbConnection = DatabaseConnection(sqlConnection)
        val system = SystemModel(dbConnection)

        for {
          _ <- system.deinstall()
          _ <- system.setup()
        } yield {
          async.complete()
        }
      case Failure(e) =>
        logger.error("Verticle couldn't be deployed.", e)
        context.fail(e)
        async.complete()
    }: Try[String] => Unit)
  }

  @After
  def after(context: TestContext) {
    val async = context.async()

    vertx.undeploy(deploymentId, {
      case Success(_) =>
        logger.info("Verticle undeployed!")
        async.complete()
      case Failure(e) =>
        logger.error("Verticle couldn't be undeployed.", e)
        context.fail(e)
        async.complete()
    }: Try[Void] => Unit)
  }

  def okTest(f: => Future[_])(implicit context: TestContext): Unit = {
    val async = context.async()
    (try f catch {
      case ex: Throwable => Future.failed(ex)
    }) onComplete {
      case Success(_) => async.complete()
      case Failure(ex) =>
        logger.error("failed test", ex)
        context.fail(ex)
        async.complete()
    }
  }

  def exceptionTest(id: String)(f: => Future[_])(implicit context: TestContext): Unit = {
    val async = context.async()
    f onComplete {
      case Success(_) =>
        val msg = s"Test with id $id should fail but got no exception."
        logger.error(msg)
        context.fail(msg)
      case Failure(ex: TestCustomException) =>
        context.assertEquals(id, ex.id)
        async.complete()
      case Failure(ex) =>
        val msg = s"Test with id $id failed but got wrong exception (${ex.getMessage})."
        logger.error(msg)
        context.fail(msg)
    }
  }

  def createClient(): HttpClient = {
    vertx.createHttpClient()
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

  def jsonResponse(p: Promise[JsonObject]): HttpClientResponse => Unit = { resp: HttpClientResponse =>
    def jsonBodyHandler(buf: Buffer): Unit = {
      if (resp.statusCode() != 200) {
        p.failure(TestCustomException(buf.toString, resp.statusMessage(), resp.statusCode()))
      } else {
        try {
          p.success(Json.fromObjectString(buf.toString))
        } catch {
          case ex: Exception => p.failure(ex)
        }
      }
    }

    resp.bodyHandler(jsonBodyHandler(_: Buffer))
  }

  private def httpJsonRequest(method: String, path: String, p: Promise[JsonObject]): HttpClientRequest = {
    httpRequest(method, path, jsonResponse(p), { x => p.failure(x) })
  }

  def httpRequest(method: String, path: String, responseHandler: HttpClientResponse => Unit, exceptionHandler: Throwable => Unit): HttpClientRequest = {
    val _method = HttpMethod.valueOf(method.toUpperCase)

    createClient()
      .request(_method, port, "localhost", path)
      .handler(responseHandler)
      .exceptionHandler(exceptionHandler)
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
      tableId <- sendRequest("POST", "/tables", postTable) map { js => js.getLong("id") }
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createStringColumnJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns", createNumberColumnJson)
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequest("POST", s"/tables/$tableId/rows")
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/1", fillStringCellJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/1/rows/2", fillStringCellJson2)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/1", fillNumberCellJson)
      _ <- sendRequest("POST", s"/tables/$tableId/columns/2/rows/2", fillNumberCellJson2)
    } yield tableId
  }
}