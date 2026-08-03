package com.campudus.tableaux.verticles

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain.{ExtendedFile, MultiLanguageValue, TableauxFile}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.helper.Path
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.testtools.TestCustomException
import com.campudus.tableaux.verticles._

import io.vertx.core.{DeploymentOptions, Vertx}
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.{HttpClient, HttpClientResponse}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.*
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.lang.scala.json.JsonObject
import io.vertx.scala.FutureHelper.futurify

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import java.net.URLEncoder
import java.util.UUID
import javax.imageio.ImageIO
import org.joda.time.DateTime
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class ThumbnailVerticleTest extends TableauxTestBase {

  // PNG re-encoding (e.g. Deflater/zlib) can differ byte-for-byte across JDK builds for identical
  // pixel data, especially for filters like Lanczos that involve floating-point resampling, which can
  // shift a channel by 1 depending on JVM/CPU rounding. Compare decoded pixels with a small per-channel
  // tolerance instead of raw bytes to avoid fixture flakiness unrelated to actual image content.
  private def assertImagesEqual(expected: Buffer, actual: Buffer): Unit = {
    val channelTolerance = 2

    def decode(buffer: Buffer): BufferedImage = ImageIO.read(new ByteArrayInputStream(buffer.getBytes))
    def channels(argb: Int): Seq[Int] = Seq(argb >> 24, argb >> 16, argb >> 8, argb).map(_ & 0xff)

    val expectedImage = decode(expected)
    val actualImage = decode(actual)

    assertEquals("Thumbnail width should match", expectedImage.getWidth, actualImage.getWidth)
    assertEquals("Thumbnail height should match", expectedImage.getHeight, actualImage.getHeight)

    for {
      x <- 0 until expectedImage.getWidth
      y <- 0 until expectedImage.getHeight
    } {
      val expectedChannels = channels(expectedImage.getRGB(x, y))
      val actualChannels = channels(actualImage.getRGB(x, y))

      (expectedChannels zip actualChannels).foreach {
        case (expectedChannel, actualChannel) =>
          assertTrue(
            s"Pixel at ($x, $y) should match within tolerance, expected: $expectedChannels but was: $actualChannels",
            Math.abs(expectedChannel - actualChannel) <= channelTolerance
          )
      }
    }
  }

  @Test
  def testThumbnailCreation(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Screen.Shot.png"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val fileMimeType = "image/png"

      val thumbnailMimeType = "image/png"
      val thumbnailWidth = 400
      val thumbnailFilter = 3 // default
      val thumbnailsDirectoryPath = tableauxConfig.thumbnailsDirectoryPath()
      val thumbnailPathExpected = s"/com/campudus/tableaux/uploads/Screen.Shot_${thumbnailWidth}_${thumbnailFilter}.png"
      val thumbnailBufferExpected =
        vertx.fileSystem.readFileBlocking(getClass.getResource(thumbnailPathExpected).toURI.getPath)

      val meta = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test Image"),
        "description" -> Json.obj("de-DE" -> "A screenshot")
      )

      for {
        file <- sendRequest("POST", "/files", meta)
        fileUuid = file.getString("uuid")
        uploadedFile <- uploadFile("PUT", s"/files/$fileUuid/de-DE", filePath, fileMimeType)
        internalName = uploadedFile.getJsonObject("internalName").getString("de-DE")
        extension = Path(internalName).extension
        internalUuid = internalName.replace(s".$extension", "")
        thumbnailName = s"${internalUuid}_${thumbnailWidth}_${thumbnailFilter}.png"
        thumbnailPath = thumbnailsDirectoryPath / Path(thumbnailName)

        doesThumbnailExistBeforeRequest <-
          vertx.fileSystem().exists(thumbnailPath.toString).asScala.map(_.booleanValue())

        thumbnailBuffer <- futurify((p: Promise[Buffer]) =>
          httpRequest(
            "GET",
            s"/files/$fileUuid/de-DE/$fileName?width=$thumbnailWidth",
            (client: HttpClient, resp: HttpClientResponse) => {
              assertEquals(200, resp.statusCode())
              assertEquals("Should get the correct MIME type", thumbnailMimeType, resp.getHeader("content-type"))

              resp.bodyHandler((buffer: Buffer) => {
                client.close()
                p.success(buffer)
              })
            },
            (client: HttpClient, x: Throwable) => {
              client.close()
              c.fail(x)
              p.failure(x)
            },
            None
          ).foreach(_.end())
        )

        doesThumbnailExistAfterRequest <-
          vertx.fileSystem().exists(thumbnailPath.toString).asScala.map(_.booleanValue())

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
        _ <- vertx.fileSystem().delete(thumbnailPath.toString()).asScala
      } yield {
        assertEquals(false, doesThumbnailExistBeforeRequest)
        assertImagesEqual(thumbnailBufferExpected, thumbnailBuffer)
        assertEquals(true, doesThumbnailExistAfterRequest)
      }
    }
  }

  @Test
  def testThumbnailCreationInvalidWidth(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Screen.Shot.png"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val fileMimeType = "image/png"

      val thumbnailWidth = -400

      val meta = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test Image"),
        "description" -> Json.obj("de-DE" -> "A screenshot")
      )

      for {
        file <- sendRequest("POST", "/files", meta)
        fileUuid = file.getString("uuid")
        _ <- uploadFile("PUT", s"/files/$fileUuid/de-DE", filePath, fileMimeType)

        _ <- futurify((p: Promise[Buffer]) =>
          httpRequest(
            "GET",
            s"/files/$fileUuid/de-DE/$fileName?width=$thumbnailWidth",
            (client: HttpClient, resp: HttpClientResponse) => {
              resp.bodyHandler((buffer: Buffer) => {
                assertEquals(400, resp.statusCode())

                client.close()

                if (resp.statusCode() != 200) {
                  p.failure(TestCustomException(buffer.toString(), resp.statusMessage(), resp.statusCode()))
                } else {
                  p.success(buffer)
                }
              })
            },
            (client: HttpClient, x: Throwable) => {
              client.close()
              c.fail(x)
              p.failure(x)
            },
            None
          ).foreach(_.end())
        )
      } yield ()
    }
  }

  @Test
  def testThumbnailCreationUnsupportedMimeType(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Scr$en Shot.pdf"
      val encodedFileName = URLEncoder.encode(fileName, "UTF-8")
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val fileMimeType = "application/pdf"

      val thumbnailWidth = 400

      val meta = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test PDF"),
        "description" -> Json.obj("de-DE" -> "A pdf")
      )

      for {
        file <- sendRequest("POST", "/files", meta)
        fileUuid = file.getString("uuid")
        _ <- uploadFile("PUT", s"/files/$fileUuid/de-DE", filePath, fileMimeType)

        _ <- futurify((p: Promise[Buffer]) =>
          httpRequest(
            "GET",
            s"/files/$fileUuid/de-DE/$encodedFileName?width=$thumbnailWidth",
            (client: HttpClient, resp: HttpClientResponse) => {
              resp.bodyHandler((buffer: Buffer) => {
                assertEquals(400, resp.statusCode())

                client.close()

                if (resp.statusCode() != 200) {
                  p.failure(TestCustomException(buffer.toString(), resp.statusMessage(), resp.statusCode()))
                } else {
                  p.success(buffer)
                }
              })
            },
            (client: HttpClient, x: Throwable) => {
              client.close()
              c.fail(x)
              p.failure(x)
            },
            None
          ).foreach(_.end())
        )
      } yield ()
    }
  }

  @Test
  def testThumbnailCreationWithFilter(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Screen.Shot.png"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val fileMimeType = "image/png"

      val thumbnailMimeType = "image/png"
      val thumbnailWidth = 400
      val thumbnailFilter = 13
      val thumbnailsDirectoryPath = tableauxConfig.thumbnailsDirectoryPath()
      val thumbnailPathExpected = s"/com/campudus/tableaux/uploads/Screen.Shot_${thumbnailWidth}_${thumbnailFilter}.png"
      val thumbnailBufferExpected =
        vertx.fileSystem.readFileBlocking(getClass.getResource(thumbnailPathExpected).toURI.getPath)

      val meta = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test Image"),
        "description" -> Json.obj("de-DE" -> "A screenshot")
      )

      for {
        file <- sendRequest("POST", "/files", meta)
        fileUuid = file.getString("uuid")
        uploadedFile <- uploadFile("PUT", s"/files/$fileUuid/de-DE", filePath, fileMimeType)
        internalName = uploadedFile.getJsonObject("internalName").getString("de-DE")
        extension = Path(internalName).extension
        internalUuid = internalName.replace(s".$extension", "")
        thumbnailName = s"${internalUuid}_${thumbnailWidth}_${thumbnailFilter}.png"
        thumbnailPath = thumbnailsDirectoryPath / Path(thumbnailName)

        doesThumbnailExistBeforeRequest <-
          vertx.fileSystem().exists(thumbnailPath.toString).asScala.map(_.booleanValue())

        thumbnailBuffer <- futurify((p: Promise[Buffer]) =>
          httpRequest(
            "GET",
            s"/files/$fileUuid/de-DE/$fileName?width=$thumbnailWidth&filter=$thumbnailFilter",
            (client: HttpClient, resp: HttpClientResponse) => {
              assertEquals(200, resp.statusCode())
              assertEquals("Should get the correct MIME type", thumbnailMimeType, resp.getHeader("content-type"))

              resp.bodyHandler((buffer: Buffer) => {
                client.close()
                p.success(buffer)
              })
            },
            (client: HttpClient, x: Throwable) => {
              client.close()
              c.fail(x)
              p.failure(x)
            },
            None
          ).foreach(_.end())
        )

        doesThumbnailExistAfterRequest <-
          vertx.fileSystem().exists(thumbnailPath.toString).asScala.map(_.booleanValue())

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
        _ <- vertx.fileSystem().delete(thumbnailPath.toString()).asScala
      } yield {
        assertEquals(false, doesThumbnailExistBeforeRequest)
        assertImagesEqual(thumbnailBufferExpected, thumbnailBuffer)
        assertEquals(true, doesThumbnailExistAfterRequest)
      }
    }
  }

  @Test
  def testThumbnailCreationInvalidFilter(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val fileName = "Screen.Shot.png"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val fileMimeType = "image/png"

      val thumbnailWidth = 400
      val thumbnailFilter = 16

      val meta = Json.obj(
        "title" -> Json.obj("de-DE" -> "Test Image"),
        "description" -> Json.obj("de-DE" -> "A screenshot")
      )

      for {
        file <- sendRequest("POST", "/files", meta)
        fileUuid = file.getString("uuid")
        _ <- uploadFile("PUT", s"/files/$fileUuid/de-DE", filePath, fileMimeType)

        _ <- futurify((p: Promise[Buffer]) =>
          httpRequest(
            "GET",
            s"/files/$fileUuid/de-DE/$fileName?width=$thumbnailWidth&filter=$thumbnailFilter",
            (client: HttpClient, resp: HttpClientResponse) => {
              resp.bodyHandler((buffer: Buffer) => {
                assertEquals(400, resp.statusCode())

                client.close()

                if (resp.statusCode() != 200) {
                  p.failure(TestCustomException(buffer.toString(), resp.statusMessage(), resp.statusCode()))
                } else {
                  p.success(buffer)
                }
              })
            },
            (client: HttpClient, x: Throwable) => {
              client.close()
              c.fail(x)
              p.failure(x)
            },
            None
          ).foreach(_.end())
        )
      } yield ()
    }
  }
}
