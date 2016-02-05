package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.{DomainObject, MultiLanguageValue}
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.{ArgumentChecker, TableauxConfig}
import io.vertx.core.http.HttpServerFileUpload
import io.vertx.ext.web.RoutingContext
import io.vertx.scala.FunctionConverters._
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.router.RouterException
import org.vertx.scala.router.routing._

import scala.concurrent.{Future, Promise}
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
  val FolderId: Regex = s"/folders/(\\d+)".r

  val FilesLang: Regex = s"/files/($langtagRegex)".r

  val FileId: Regex = s"/files/($uuidRegex)".r
  val FileIdMerge: Regex = s"/files/($uuidRegex)/merge".r
  val FileIdLang: Regex = s"/files/($uuidRegex)/($langtagRegex)".r
  val FileIdLangStatic: Regex = s"/files/($uuidRegex)/($langtagRegex)/.*".r

  override def routes(implicit context: RoutingContext): Routing = {

    /**
      * Create folder
      */
    case Post("/folders") => asyncGetReply {
      for {
        json <- getJson(context)
        name = json.getString("name")
        description = json.getString("description")
        parent = getNullableField("parent")(json)

        added <- controller.addNewFolder(name, description, parent)
        // TODO sortByLangtag should be removed and the real folder should be fetched
      } yield added
    }

    /**
      * Retrieve root folder
      */
    case Get("/folders") => asyncGetReply {
      import ArgumentChecker._

      //TODO this will result in a unhandled exception error
      val sortByLangtag = checked(isDefined(getStringParam("langtag", context), "langtag"))
      controller.retrieveRootFolder(sortByLangtag)
    }

    /**
      * Retrieve folder
      */
    case Get(FolderId(id)) => asyncGetReply {
      import ArgumentChecker._

      //TODO this will result in a unhandled exception error
      val sortByLangtag = checked(isDefined(getStringParam("langtag", context), "langtag"))
      controller.retrieveFolder(id.toLong, sortByLangtag)
    }

    /**
      * Change folder
      */
    case Put(FolderId(id)) => asyncGetReply {
      for {
        json <- getJson(context)
        name = json.getString("name")
        description = json.getString("description")
        parent = getNullableField("parent")(json)

        changed <- controller.changeFolder(id.toLong, name, description, parent)
      // TODO sortByLangtag should be removed and the real folder should be fetched
      } yield changed
    }

    /**
      * Delete folder and its files
      */
    case Delete(FolderId(id)) => asyncGetReply {
      controller.deleteFolder(id.toLong)
    }

    /**
      * Create file handle
      */
    case Post("/files") => asyncGetReply {
      for {
        json <- getJson(context)

        title = MultiLanguageValue[String](getNullableField("title")(json))
        description = MultiLanguageValue[String](getNullableField("description")(json))
        externalName = MultiLanguageValue[String](getNullableField("externalName")(json))
        folder = getNullableField("folder")(json)

        added <- controller.addFile(title, description, externalName, folder)
      } yield added
    }

    /**
      * Retrieve file meta information
      */
    case Get(FileId(uuid)) => asyncGetReply {
      controller.retrieveFile(UUID.fromString(uuid)).map({ case (file, _) => file })
    }

    /**
      * Serve file
      */
    case Get(FileIdLangStatic(uuid, langtag)) => AsyncReply {
      for {
        (file, paths) <- controller.retrieveFile(UUID.fromString(uuid))
      } yield {
        val absolute = config.isWorkingDirectoryAbsolute

        val mimeType = file.file.mimeType.get(langtag)
        val path = paths.get(langtag).get

        Header("Content-type", mimeType.get, SendFile(path.toString(), absolute))
      }
    }

    /**
      * Change file meta information
      */
    case Put(FileId(uuid)) => asyncGetReply {
      for {
        json <- getJson(context)

        title = MultiLanguageValue[String](getNullableField("title")(json))
        description = MultiLanguageValue[String](getNullableField("description")(json))
        externalName = MultiLanguageValue[String](getNullableField("externalName")(json))
        internalName = MultiLanguageValue[String](getNullableField("internalName")(json))
        mimeType = MultiLanguageValue[String](getNullableField("mimeType")(json))

        folder = getNullableField[FolderId]("folder")(json)

        changed <- controller.changeFile(UUID.fromString(uuid), title, description, externalName, internalName, mimeType, folder)
      } yield changed
    }

    /**
      * Replace/upload language specific file and its meta information
      */
    case Put(FileIdLang(uuid, langtag)) => asyncGetReply {
      handleUpload(context, { (action: UploadAction) =>
        controller.replaceFile(UUID.fromString(uuid), langtag, action)
      })
    }

    /**
      * File merge
      */
    case Post(FileIdMerge(uuid)) => asyncGetReply {
      for {
        json <- getJson(context)
        langtag = checked(hasString("langtag", json))
        mergeWith = UUID.fromString(checked(hasString("mergeWith", json)))

        merged <- controller.mergeFile(UUID.fromString(uuid), langtag, mergeWith)
      } yield merged
    }

    /**
      * Delete file
      */
    case Delete(FileId(uuid)) => asyncGetReply {
      controller.deleteFile(UUID.fromString(uuid))
    }

    /**
      * Delete language specific stuff
      */
    case Delete(FileIdLang(uuid, langtag)) => asyncGetReply {
      controller.deleteFile(UUID.fromString(uuid), langtag)
    }
  }

  private def getNullableField[A](field: String)(implicit json: JsonObject): Option[A] = {
    Option(json.getValue(field).asInstanceOf[A])
  }

  private def handleUpload(implicit context: RoutingContext, fn: (UploadAction) => Future[DomainObject]): Future[DomainObject] = {
    logger.info(s"Handle upload for ${context.request().absoluteURI()} ${context.fileUploads()}")

    val req = context.request()

    val p = Promise[DomainObject]()

    val timerId = vertx.setTimer(10000L, { timerId: java.lang.Long =>
      p.failure(RouterException(message = "No valid file upload received", id = "errors.upload.invalidRequest", statusCode = 400))
    })

    req.setExpectMultipart(true)

    // TODO this only can handle one file upload per request
    req.uploadHandler({ upload: HttpServerFileUpload =>
      logger.info("Received a file upload")

      vertx.cancelTimer(timerId)

      val setExceptionHandler = (exHandler: Throwable => Unit) => upload.exceptionHandler(exHandler)
      val setEndHandler = (fn: () => Unit) => upload.endHandler(fn())
      val setStreamToFile = (fPath: String) => upload.streamToFileSystem(fPath)

      val action = UploadAction(upload.filename(), upload.contentType(), setExceptionHandler, setEndHandler, setStreamToFile)

      fn(action).map(p.success).recoverWith({ case e => logger.error("Upload failed", e); p.failure(e); Future.failed(e) })
    })

    p.future
  }

}