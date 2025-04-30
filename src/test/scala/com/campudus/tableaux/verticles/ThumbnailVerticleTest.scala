package com.campudus.tableaux.verticles

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain.{ExtendedFile, MultiLanguageValue, TableauxFile}
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.testtools.TableauxTestBase
import com.campudus.tableaux.verticles._

import io.vertx.core.buffer.Buffer
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.FutureHelper.futurify
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import io.vertx.scala.core.http.{HttpClient, HttpClientResponse}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

import java.util.UUID
import org.joda.time.DateTime
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class ThumbnailVerticleTest extends TableauxTestBase {

  @Test
  def testThumbnailCreation(implicit c: TestContext): Unit = {
    okTest {
      val fileName = "Screen.Shot.png"
      val filePath = s"/com/campudus/tableaux/uploads/$fileName"
      val fileMimeType = "image/png"

      val thumbnailMimeType = "image/png"
      val thumbnailWidth = 400
      val thumbnailsDirectoryPath = tableauxConfig.thumbnailsDirectoryPath
      val thumbnailPathExpected = s"/com/campudus/tableaux/uploads/Screen.Shot_$thumbnailWidth.png"
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
        thumbnailName = s"${internalUuid}_$thumbnailWidth.png"
        thumbnailPath = thumbnailsDirectoryPath / Path(thumbnailName)

        doesThumbnailExistBeforeRequest <- vertx.fileSystem().existsFuture(thumbnailPath.toString)

        thumbnailBuffer <- futurify((p: Promise[Buffer]) =>
          httpRequest(
            "GET",
            s"/files/$fileUuid/de-DE/$fileName?width=$thumbnailWidth",
            (client: HttpClient, resp: HttpClientResponse) => {
              assertEquals(200, resp.statusCode())
              assertEquals("Should get the correct MIME type", Some(thumbnailMimeType), resp.getHeader("content-type"))

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
          ).end()
        )

        doesThumbnailExistAfterRequest <- vertx.fileSystem().existsFuture(thumbnailPath.toString)

        _ <- sendRequest("DELETE", s"/files/$fileUuid")
        _ <- vertx.fileSystem().deleteFuture(thumbnailPath.toString())
      } yield {
        assertEquals(doesThumbnailExistBeforeRequest, false)
        assertEquals("Should be the expected file", thumbnailBufferExpected, thumbnailBuffer)
        assertEquals(doesThumbnailExistAfterRequest, true)
      }
    }
  }
}
