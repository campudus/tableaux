package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.DomainObject
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.router.RouterException
import org.vertx.scala.router.routing._

import scala.concurrent.Promise
import scala.util.matching.Regex

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
  val FileId: Regex = s"/files/($uuidRegex)".r
  val FileIdStatic: Regex = s"/files/($uuidRegex)/.*".r

  override def routes(implicit req: HttpServerRequest): Routing = {
    case Post("/folders") => asyncSetReply({
      getJson(req) flatMap { implicit json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = getNullableField("parent")

        controller.addNewFolder(name, description, parent)
      }
    })
    case Get("/folders") => asyncGetReply(controller.retrieveRootFolder())
    case Get(FolderId(id)) => asyncGetReply(controller.retrieveFolder(id.toLong))
    case Put(FolderId(id)) => asyncSetReply({
      getJson(req) flatMap { implicit json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = getNullableField("parent")

        controller.changeFolder(id.toLong, name, description, parent)
      }
    })
    case Delete(FolderId(id)) => asyncSetReply({controller.deleteFolder(id.toLong)})

    case Post("/files") => asyncSetReply({
      val p = Promise[DomainObject]()

      val timerId = vertx.setTimer(10000, { timerId =>
        p.failure(RouterException(message = "No valid file upload received", id = "errors.upload.invalidRequest", statusCode = 400))
      })

      req.expectMultiPart(expect = true)

      req.uploadHandler({ upload =>
        logger.debug("Received a file upload")
        vertx.cancelTimer(timerId)

        val oldFileName = upload.filename()
        val mimeType = upload.contentType()
        val setExceptionHandler = (exHandler: Throwable => Unit) => upload.exceptionHandler(exHandler)
        val setEndHandler = (fn: () => Unit) => upload.endHandler(fn())
        val setStreamToFile = (fPath: String) => upload.streamToFileSystem(fPath)

        for {
          file <- controller.uploadFile(UploadAction(oldFileName, mimeType, setExceptionHandler, setEndHandler, setStreamToFile))
        } yield {
          p.success(file)
        }
      })

      p.future
    })
    case Get(FileId(uuid)) => asyncGetReply({
      controller.retrieveFile(UUID.fromString(uuid)) map { result => result._1 }
    })
    case Get(FileIdStatic(uuid)) => {
      AsyncReply({
        for {
          (file, path) <- controller.retrieveFile(UUID.fromString(uuid))
        } yield {
          val absolute = config.workingDirectory.startsWith("/")
          Header("Content-type", file.file.mimeType, SendFile(path.toString(), absolute))
        }
      })
    }
    case Put(FileId(uuid)) => asyncSetReply({
      getJson(req) flatMap { implicit json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val folder = getNullableField("folder")

        controller.changeFile(UUID.fromString(uuid), name, description, folder)
      }
    })
    case Delete(FileId(uuid)) => asyncSetReply({
      controller.deleteFile(UUID.fromString(uuid))
    })
  }

  def getNullableField[A](field: String)(implicit json: JsonObject): Option[A] = {
    Option(json.getField[A](field))
  }
}