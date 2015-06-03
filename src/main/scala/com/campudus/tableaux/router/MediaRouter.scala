package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.FolderModel.FolderId
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
  /**
   * Regex for a UUID Version 4
   */
  val uuidRegex: String = "[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"

  val FolderId: Regex = "/folders/(\\d+)".r
  val FileId: Regex = s"/files/($uuidRegex)".r
  val FileIdStatic: Regex = s"/files/($uuidRegex)/.*".r

  override def routes(implicit req: HttpServerRequest): Routing = {
    case Post("/folders") => asyncSetReply({
      getJson(req) flatMap { json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = retrieveNullableField(json)("parent")

        controller.addNewFolder(name, description, parent)
      }
    })
    case Get(FolderId(id)) => asyncGetReply(controller.retrieveFolder(id.toLong))
    case Put(FolderId(id)) => asyncSetReply({
      getJson(req) flatMap { json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = retrieveNullableField(json)("parent")

        controller.changeFolder(id.toLong, name, description, parent)
      }
    })

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
      AsyncReply(for {
        (file, path) <- controller.retrieveFile(UUID.fromString(uuid))
      } yield Header("Content-type", file.file.mimeType, SendFile(path.toString())))
    }
    case Put(FileId(uuid)) => asyncSetReply({
      getJson(req) flatMap { json =>
        val name = json.getString("name")
        val description = json.getString("description")
        val folder: Option[FolderId] = retrieveNullableField(json)("folder")

        controller.changeFile(UUID.fromString(uuid), name, description, folder)
      }
    })
    case Delete(FileId(uuid)) => asyncSetReply({
      controller.deleteFile(UUID.fromString(uuid))
    })
  }

  def retrieveNullableField[A](json: JsonObject)(field: String): Option[A] = {
    Option(json.getField[A](field))
  }
}