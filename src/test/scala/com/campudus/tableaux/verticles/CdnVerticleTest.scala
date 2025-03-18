package com.campudus.tableaux.verticles

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain.{ExtendedFile, MultiLanguageValue, TableauxFile}
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.testtools.{TestAssertionHelper, TestCustomException}
import com.campudus.tableaux.verticles._

import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import io.vertx.scala.ext.web.client.{HttpRequest, HttpResponse, WebClient}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import java.util.UUID
import org.joda.time.DateTime
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.mockito.Mockito.{mock, reset, verify, when}
import org.scalatestplus.mockito.MockitoSugar

@RunWith(classOf[VertxUnitRunner])
class CdnVerticleTest extends VertxAccess with MockitoSugar {
  override val vertx: Vertx = Vertx.vertx()

  val cdnConfig = Json.obj("url" -> "http://my.cdn.url", "apiKey" -> "123456")

  val mockClient = mock[WebClient]
  val mockRequest = mock[HttpRequest[Buffer]]
  val eventClient = new EventClient(vertx)

  private var deploymentId: String = "CdnVerticleTest"

  @Before
  def before(context: TestContext) {
    val async = context.async()

    val options = DeploymentOptions()
      .setConfig(Json.emptyObj())

    vertx
      .deployVerticleFuture(new CdnVerticle(cdnConfig, Option(mockClient)), options)
      .onComplete({
        case Success(id) =>
          logger.info(s"Verticle deployed with ID $id")
          this.deploymentId = id
          async.complete()

        case Failure(e) =>
          logger.error("Verticle couldn't be deployed.", e)
          context.fail(e)
          async.complete()
      })
  }

  @After
  def after(context: TestContext) {
    val async = context.async()

    reset(mockClient)
    reset(mockRequest)

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
  def testPurgeCdnFileUrl(implicit context: TestContext): Unit = {
    val file = TableauxFile(
      UUID.randomUUID(),
      folders = Seq(1, 2, 3),
      title = MultiLanguageValue("de-DE" -> "title"),
      description = MultiLanguageValue("de-DE" -> "description"),
      internalName = MultiLanguageValue("de-DE" -> "internalName"),
      externalName = MultiLanguageValue("de-DE" -> "externalName"),
      mimeType = MultiLanguageValue("de-DE" -> "mimeType"),
      createdAt = Some(DateTime.now()),
      updatedAt = Some(DateTime.now())
    )

    val extendedFile = ExtendedFile(file)

    val mockResponse = mock[HttpResponse[Buffer]]

    when(mockClient.postAbs("http://my.cdn.url/purge")).thenReturn(mockRequest)
    when(mockRequest.sendFuture()).thenReturn(Future.successful(mockResponse))

    okTest {
      for {
        _ <- eventClient.fileChanged(extendedFile)
      } yield {
        verify(mockClient).postAbs("http://my.cdn.url/purge")
        verify(mockRequest).addQueryParam("url", s"http://my.cdn.url/${file.uuid}/*")
      }
    }
  }
}
