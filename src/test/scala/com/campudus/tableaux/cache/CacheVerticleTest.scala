package com.campudus.tableaux.cache

import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.testtools.{TestAssertionHelper, TestCustomException}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{After, Before, Test}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@RunWith(classOf[VertxUnitRunner])
class CacheVerticleTest extends VertxAccess with TestAssertionHelper {

  override val vertx: Vertx = Vertx.vertx()

  private var deploymentId: String = ""

  @Before
  def before(context: TestContext) {
    val async = context.async()

    val options = DeploymentOptions()
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

    vertx
      .deployVerticleFuture(ScalaVerticle.nameForVerticle[CacheVerticle], options)
      .onComplete(completionHandler)
  }

  @After
  def after(context: TestContext) {
    val async = context.async()

    vertx
      .undeployFuture(deploymentId)
      .onComplete({
        case Success(_) =>
          logger.info("Verticle undeployed!")
          async.complete()

        case Failure(e) =>
          logger.error("Verticle couldn't be undeployed!", e)
          context.fail(e)
          async.complete()
      })
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
      vertx
        .eventBus()
        .sendFuture[JsonObject](
          CacheVerticle.ADDRESS_RETRIEVE,
          Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
        )
        .flatMap(_ => Future.failed(TestCustomException("shouldn't succeed", "", 0)))
        .recoverWith({
          case e @ TestCustomException("shouldn't succeed", _, _) =>
            Future.failed(e)
          case _: Throwable =>
            Future.successful(())
        })
    }
  }

  @Test
  def testRetrievingCachedCellValue(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo"))
          )

        result <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )
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
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_INVALIDATE_CELL,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )
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
  def testInvalidateColumn(implicit context: TestContext): Unit = {
    okTest {
      for {
        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo"))
          )
        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_INVALIDATE_COLUMN,
            Json.obj("tableId" -> 1, "columnId" -> 1)
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )
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
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](CacheVerticle.ADDRESS_INVALIDATE_ALL, Json.emptyObj())

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )
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
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo111"))
          )
        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo112"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 2, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo211"))
          )
        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 2, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo212"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_INVALIDATE_TABLE,
            Json.obj("tableId" -> 2)
          )

        value <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.failed(ex)
          })

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 2, "columnId" -> 1, "rowId" -> 1)
          )
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
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1, "value" -> Json.obj("test" -> "hallo111"))
          )
        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_SET,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2, "value" -> Json.obj("test" -> "hallo112"))
          )

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_INVALIDATE_ROW,
            Json.obj("tableId" -> 1, "rowId" -> 2)
          )

        value <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 1)
          )
          .recoverWith({
            case ex =>
              logger.info("Retrieving cache entry failed", ex)
              Future.failed(ex)
          })

        _ <- vertx
          .eventBus()
          .sendFuture[JsonObject](
            CacheVerticle.ADDRESS_RETRIEVE,
            Json.obj("tableId" -> 1, "columnId" -> 1, "rowId" -> 2)
          )
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
