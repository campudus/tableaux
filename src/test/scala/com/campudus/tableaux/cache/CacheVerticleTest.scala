package com.campudus.tableaux.cache

import com.campudus.tableaux.testtools.TestAssertionHelper
import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.eventbus.Message
import io.vertx.core.{AsyncResult, DeploymentOptions, Handler, Vertx}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.FunctionConverters._
import org.junit.runner.RunWith
import org.junit.{After, Before, Test}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

@RunWith(classOf[VertxUnitRunner])
class CacheVerticleTest extends LazyLogging with TestAssertionHelper {

  val verticle = new CacheVerticle
  implicit lazy val executionContext = verticle.executionContext

  val vertx: Vertx = Vertx.vertx()
  private var deploymentId: String = ""

  @Before
  def before(context: TestContext) {
    val async = context.async()

    val options = new DeploymentOptions()
      .setConfig(Json.emptyObj())

    val completionHandler = {
      case Success(id) =>
        logger.info(s"Verticle deployed with ID $id")
        this.deploymentId = id

        async.complete()

      case Failure(e) =>
        logger.error("Verticle couldn't be deployed.", e)
        context.fail(e)
        async.complete()
    }: Try[String] => Unit

    vertx.deployVerticle(verticle, options, completionHandler)
  }

  @After
  def after(context: TestContext) {
    val async = context.async()

    vertx.undeploy(
      deploymentId, {
        case Success(_) =>
          logger.info("Verticle undeployed!")
          vertx.close({
            case Success(_) =>
              logger.info("Vertx closed!")
              async.complete()
            case Failure(e) =>
              logger.error("Vertx couldn't be closed!", e)
              context.fail(e)
              async.complete()
          }: Try[Void] => Unit)
        case Failure(e) =>
          logger.error("Verticle couldn't be undeployed!", e)
          context.fail(e)
          async.complete()
      }: Try[Void] => Unit
    )
  }

  def okTest(f: => Future[_])(implicit context: TestContext): Unit = {
    val async = context.async()
    (try {
      f
    } catch {
      case ex: Throwable => Future.failed(ex)
    }) onComplete {
      case Success(_) => async.complete()
      case Failure(ex) =>
        logger.error("failed test", ex)
        context.fail(ex)
        async.complete()
    }
  }

  @Test
  def testRetrievingNonExistingCell(implicit context: TestContext): Unit = {
    okTest {
      val promise = Promise[Unit]()

      val replyHandler: Try[Message[JsonObject]] => Unit = {
        case Success(message) =>
          fail("shouldn't succeed")
          promise.success(())
        case Failure(ex) =>
          promise.success(())
      }

      vertx
        .eventBus()
        .send(CacheVerticle.ADDRESS_RETRIEVE, Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1), replyHandler)

      promise.future
    }
  }

  @Test
  def testRetrievingCachedCellValue(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        result <- vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]])
      } yield {
        assertEquals(Json.obj("test" -> "hallo"), result.body().getJsonObject("value"))
      }
    }
  }

  @Test
  def testRetrievingInvalidatedCellValue(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        _ <- vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_INVALIDATE_CELL,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]])
        _ <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .flatMap({ json =>
            {
              fail("Shouldn't reply anything")
              Future.failed(new Exception("Shouldn't reply anything"))
            }
          })
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.successful(())
          })
      } yield ()
    }
  }

  @Test
  def testInvalidateColumn(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )

        _ <- vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_INVALIDATE_COLUMN,
                Json.obj("tableId" -> 1, "columnId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]])

        _ <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .flatMap({ json =>
            fail("Shouldn't reply anything")
            Future.failed(new Exception("Shouldn't reply anything"))
          })
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.successful(())
          })
      } yield ()
    }
  }

  @Test
  def testInvalidateWholeCache(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )

        _ <- vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_INVALIDATE_ALL, Json.emptyObj(), _: Handler[AsyncResult[Message[JsonObject]]])

        _ <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .flatMap({ json =>
            fail("Shouldn't reply anything")
            Future.failed(new Exception("Shouldn't reply anything"))
          })
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.successful(())
          })
      } yield ()
    }
  }

  @Test
  def testInvalidateTable(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo111")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo112")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )

        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 2, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo211")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 2, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo212")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )

        _ <- vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_INVALIDATE_TABLE,
                Json.obj("tableId" -> 2),
                _: Handler[AsyncResult[Message[JsonObject]]])

        value <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.failed(ex)
          })

        _ <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 2, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .flatMap({ json =>
            fail("Shouldn't reply anything")
            Future.failed(new Exception("Shouldn't reply anything"))
          })
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.successful(())
          })
      } yield {
        assertEquals(Json.obj("test" -> "hallo111"), value.body.getJsonObject("value"))
      }
    }
  }

  @Test
  def testInvalidateRow(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo111")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )
        _ <- vertx
          .eventBus()
          .send(
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo112")),
            _: Handler[AsyncResult[Message[JsonObject]]]
          )

        _ <- vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_INVALIDATE_ROW,
                Json.obj("tableId" -> 1, "rowId" -> 2),
                _: Handler[AsyncResult[Message[JsonObject]]])

        value <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.failed(ex)
          })

        _ <- (vertx
          .eventBus()
          .send(CacheVerticle.ADDRESS_RETRIEVE,
                Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2),
                _: Handler[AsyncResult[Message[JsonObject]]]))
          .flatMap({ json =>
            fail("Shouldn't reply anything")
            Future.failed(new Exception("Shouldn't reply anything"))
          })
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.successful(())
          })
      } yield {
        assertEquals(Json.obj("test" -> "hallo111"), value.body.getJsonObject("value"))
      }
    }
  }
}
