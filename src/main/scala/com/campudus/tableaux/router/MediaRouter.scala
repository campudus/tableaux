package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.{MultiLanguageValue, DomainObject}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.router.RouterException
import org.vertx.scala.router.routing._

import scala.concurrent.{Future, Promise}
import scala.util.matching.Regex
import scala.util.{Failure, Success}

sealed trait FileAction

case class UploadAction(fileName: String,
                        mimeType: String,
                        exceptionHandler: (Throwable => Unit) => _,
                        endHandler: (() => Unit) => _,
                        streamToFile: (String) => _) extends FileAction

object MediaRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => MediaController): MediaRouter = {
    new MediaRouter(config, controllerCurry(config))
  }
}

class MediaRouter(override val config: TableauxConfig, val controller: MediaController) extends BaseRouter {
  val FolderId: Regex = "/folders/(\\d+)".r

  val FilesLang: Regex = s"/files/($langtagRegex)".r

  val FileId: Regex = s"/files/($uuidRegex)".r
  val FileIdLang: Regex = s"/files/($uuidRegex)/($langtagRegex)".r
  val FileIdLangStatic: Regex = s"/files/($uuidRegex)/($langtagRegex)/.*".r

  override def routes(implicit req: HttpServerRequest): Routing = {
    /**
     * Create folder
     */
    case Post("/folders") => asyncSetReply({
      getJson(req) flatMap { implicit json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = getNullableField("parent")

        controller.addNewFolder(name, description, parent)
      }
    })

    /**
     * Retrieve root folder
     */
    case Get("/folders") => asyncGetReply(controller.retrieveRootFolder())

    /**
     * Retrieve folder
     */
    case Get(FolderId(id)) => asyncGetReply(controller.retrieveFolder(id.toLong))

    /**
     * Change folder
     */
    case Put(FolderId(id)) => asyncSetReply({
      getJson(req) flatMap { implicit json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = getNullableField("parent")

        controller.changeFolder(id.toLong, name, description, parent)
      }
    })

    /**
     * Delete folder and its files
     */
    case Delete(FolderId(id)) => asyncSetReply(controller.deleteFolder(id.toLong))

    /**
     * Create file handle
     */
    case Post("/files") => asyncSetReply({
      getJson(req) flatMap { implicit json =>
        val title = MultiLanguageValue[String](getNullableField("title"))
        val description = MultiLanguageValue[String](getNullableField("description"))
        val folder = getNullableField("folder")

        controller.addFile(title, description, folder)
      }
    })

    /**
     * Retrieve file meta information
     */
    case Get(FileId(uuid)) => asyncGetReply(controller.retrieveFile(UUID.fromString(uuid)).map({ case (file, _) => file }))

    /**
     * Serve file
     */
    case Get(FileIdLangStatic(uuid, langtag)) => AsyncReply({
      for {
        (file, paths) <- controller.retrieveFile(UUID.fromString(uuid))
      } yield {
        val absolute = config.workingDirectory.startsWith("/")

        val mimeType = file.file.mimeType.get(langtag)
        val path = paths.get(langtag).get

        // TODO 404 if no path found
        Header("Content-type", mimeType.get, SendFile(path.toString(), absolute))
      }
    })

    /**
     * Change file meta information
     */
    case Put(FileId(uuid)) => asyncSetReply({
      getJson(req) flatMap { implicit json =>

        val title = MultiLanguageValue[String](json.getObject("title"))
        val description = MultiLanguageValue[String](json.getObject("description"))

        val folder = getNullableField("folder")

        controller.changeFile(UUID.fromString(uuid), title, description, folder)
      }
    })

    /**
     * Replace/upload language specific file and its meta information
     */
    case Put(FileIdLang(uuid, langtag)) => {
      logger.info(s"PUT FileIdLang $uuid $langtag")

      handleUpload(req, (action: UploadAction) => {
        logger.info(s"Call replaceFile $uuid $langtag")
        controller.replaceFile(UUID.fromString(uuid), langtag, action)
      })
    }

    /**
     * Delete file
     */
    case Delete(FileId(uuid)) => asyncSetReply(controller.deleteFile(UUID.fromString(uuid)))

    /**
     * Delete language specific stuff
     */
    case Delete(FileIdLang(uuid, langtag)) => asyncSetReply(controller.deleteFile(UUID.fromString(uuid), langtag))
  }

  def getNullableField[A](field: String)(implicit json: JsonObject): Option[A] = {
    Option(json.getField[A](field))
  }

  def handleUpload(implicit req: HttpServerRequest, fn: (UploadAction) => Future[DomainObject]): AsyncReply = asyncSetReply({
    logger.info(s"Handle upload for ${req.absoluteURI().toString}")

    val p = Promise[DomainObject]()

    val timerId = vertx.setTimer(10000, { timerId =>
      p.failure(RouterException(message = "No valid file upload received", id = "errors.upload.invalidRequest", statusCode = 400))
    })

    req.expectMultiPart(expect = true)

    req.uploadHandler({ upload =>
      logger.info("Received a file upload")

      vertx.cancelTimer(timerId)

      val setExceptionHandler = (exHandler: Throwable => Unit) => upload.exceptionHandler(exHandler)
      val setEndHandler = (fn: () => Unit) => upload.endHandler(fn())
      val setStreamToFile = (fPath: String) => upload.streamToFileSystem(fPath)

      val action = UploadAction(upload.filename(), upload.contentType(), setExceptionHandler, setEndHandler, setStreamToFile)

      logger.info("Call fn(action)")
      fn(action).map(p.success)
    })

    p.future
  })
}