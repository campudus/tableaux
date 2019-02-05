package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.{DomainObject, MultiLanguageValue}
import com.campudus.tableaux.helper.{AsyncReply, Header, SendFile}
import io.vertx.scala.core.http.{HttpServerFileUpload, HttpServerRequest}
import io.vertx.scala.ext.web.handler.BodyHandler
import io.vertx.scala.ext.web.{Router, RoutingContext}
import org.vertx.scala.core.json.JsonObject

import scala.concurrent.{Future, Promise}

sealed trait FileAction

case class UploadAction(
    fileName: String,
    mimeType: String,
    exceptionHandler: (Throwable => Unit) => _,
    endHandler: (() => Unit) => _,
    streamToFile: String => _
) extends FileAction

object MediaRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => MediaController): MediaRouter = {
    new MediaRouter(config, controllerCurry(config))
  }
}

class MediaRouter(override val config: TableauxConfig, val controller: MediaController) extends BaseRouter {

  protected val FOLDER_ID = """(?<folderId>[\d]+)"""

  val Folder: String = s"""/folders/$FOLDER_ID"""
  val Folders: String = s"/folders"

  val FilesLang: String = s"/files/$langtagRegex"

  val Files: String = s"/files"
  val File: String = s"/files/$uuidRegex"
  val FileMerge: String = s"/files/$uuidRegex/merge"
  val FileLang: String = s"/files/$uuidRegex/$langtagRegex"
  val FileLangStatic: String = s"/files/$uuidRegex/$langtagRegex/.*"

  def route: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    router.get(Folders).handler(retrieveRootFolder)
    router.getWithRegex(Folder).handler(retrieveFolder)
    router.getWithRegex(File).handler(retrieveFile)
    router.getWithRegex(FileLangStatic).handler(serveFile)

    router.deleteWithRegex(Folder).handler(deleteFolder)
    router.deleteWithRegex(File).handler(deleteFile)
    router.deleteWithRegex(FileLang).handler(deleteFileLang)

    // route for file uploading doesn't need a handler yet
    // TODO change to BodyHandler uploading
    // router.put("/files/*").handler(BodyHandler.create().setUploadsDirectory(uploadsDirectory.path))
    // all following routes may require Json in the request body
    val bodyHandler = BodyHandler.create()
    router.putWithRegex(File).handler(bodyHandler)
    router.post("/files/*").handler(bodyHandler)
    router.put("/folders/*").handler(bodyHandler)
    router.post("/folders/*").handler(bodyHandler)

    router.putWithRegex(FileLang).handler(uploadFile)

    router.post(Folders).handler(createFolder)
    router.putWithRegex(Folder).handler(updateFolder)
    router.post(Files).handler(createFile)
    router.postWithRegex(FileMerge).handler(mergeFile)
    router.putWithRegex(File).handler(updateFile)

    router
  }

  private def getFolderId(context: RoutingContext): Option[Long] = {
    getLongParam("folderId", context)
  }

  private def createFolder(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)
        val name = json.getString("name")
        val description = json.getString("description")
        val parent = getNullableLong("parent")(json)
        // TODO sortByLangtag should be removed and the real folder should be fetched
        controller.addNewFolder(name, description, parent)
      }
    )
  }

  private def retrieveRootFolder(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        val sortByLangtag = checked(isDefined(getStringParam("langtag", context), "langtag"))
        controller.retrieveRootFolder(sortByLangtag)
      }
    )
  }

  private def retrieveFolder(context: RoutingContext): Unit = {
    for {
      folderId <- getFolderId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val sortByLangtag = checked(isDefined(getStringParam("langtag", context), "langtag"))
          controller.retrieveFolder(folderId, sortByLangtag)
        }
      )
    }
  }

  private def updateFolder(context: RoutingContext): Unit = {
    for {
      folderId <- getFolderId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val name = json.getString("name")
          val description = json.getString("description")
          val parent = getNullableLong("parent")(json)
          // TODO sortByLangtag should be removed and the real folder should be fetched
          controller.changeFolder(folderId, name, description, parent)
        }
      )
    }
  }

  /**
    * Delete folder and its files
    */
  private def deleteFolder(context: RoutingContext): Unit = {
    for {
      folderId <- getFolderId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteFolder(folderId)
        }
      )
    }
  }

  /**
    * Create file handle
    */
  private def createFile(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)
        val title = MultiLanguageValue[String](getNullableObject("title")(json))
        val description = MultiLanguageValue[String](getNullableObject("description")(json))
        val externalName = MultiLanguageValue[String](getNullableObject("externalName")(json))
        val folder = getNullableLong("folder")(json)
        controller.addFile(title, description, externalName, folder)
      }
    )
  }

  /**
    * Retrieve file meta information
    */
  private def retrieveFile(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
    } yield {
      sendReply(context, asyncGetReply {
        controller.retrieveFile(UUID.fromString(fileUuid)).map({ case (file, _) => file })
      })
    }
  }

  private def serveFile(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        AsyncReply {
          for {
            (file, paths) <- controller.retrieveFile(UUID.fromString(fileUuid))
          } yield {
            val absolute = config.isWorkingDirectoryAbsolute

            val mimeType = file.file.mimeType.get(langtag)
            val path = paths(langtag)

            Header("Content-type", mimeType.get, SendFile(path.toString(), absolute))
          }
        }
      )
    }
  }

  /**
    * Update file meta information
    */
  private def updateFile(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val title = MultiLanguageValue[String](getNullableObject("title")(json))
          val description = MultiLanguageValue[String](getNullableObject("description")(json))
          val externalName = MultiLanguageValue[String](getNullableObject("externalName")(json))
          val internalName = MultiLanguageValue[String](getNullableObject("internalName")(json))
          val mimeType = MultiLanguageValue[String](getNullableObject("mimeType")(json))

          val folder = getNullableLong("folder")(json)
          val uuid = UUID.fromString(fileUuid)

          controller.changeFile(uuid, title, description, externalName, internalName, mimeType, folder)
        }
      )
    }
  }

  /**
    * Replace/upload language specific file and its meta information
    */
  private def uploadFile(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          handleUpload(context,
                       (action: UploadAction) => controller.replaceFile(UUID.fromString(fileUuid), langtag, action))
        }
      )
    }
  }

  private def mergeFile(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val langtag = checked(hasString("langtag", json))
          val mergeWith = UUID.fromString(checked(hasString("mergeWith", json)))

          controller.mergeFile(UUID.fromString(fileUuid), langtag, mergeWith)
        }
      )
    }
  }

  private def deleteFile(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteFile(UUID.fromString(fileUuid))
        }
      )
    }
  }

  private def deleteFileLang(context: RoutingContext): Unit = {
    for {
      fileUuid <- getUUID(context)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteFile(UUID.fromString(fileUuid), langtag)
        }
      )
    }
  }

  private def getNullableObject(field: String)(implicit json: JsonObject): Option[JsonObject] = {
    Option(json.getJsonObject(field))
  }

  private def getNullableLong(field: String)(implicit json: JsonObject): Option[Long] = {
    Option(json.getLong(field)).map(_.toLong)
  }

  private def handleUpload(implicit context: RoutingContext,
                           fn: UploadAction => Future[DomainObject]): Future[DomainObject] = {

    val req: HttpServerRequest = context.request().setExpectMultipart(true)

    logger.info(s"Handle upload for ${req.absoluteURI()}")

    val p = Promise[DomainObject]()

    val timerId = vertx.setTimer(
      10000L,
      _ => {
        p.failure(
          RouterException(message = "No valid file upload received",
                          id = "errors.upload.invalidRequest",
                          statusCode = 400))
      }
    )

    // TODO this only can handle one file upload per request
    req.uploadHandler({ upload: HttpServerFileUpload =>
      {
        logger.info("Received a file upload")

        vertx.cancelTimer(timerId)

        val setExceptionHandler = (exHandler: Throwable => Unit) => upload.exceptionHandler(t => exHandler(t))
        val setEndHandler = (fn: () => Unit) => upload.endHandler(_ => fn())
        val setStreamToFile = (fPath: String) => upload.streamToFileSystem(fPath)

        val action =
          UploadAction(upload.filename(), upload.contentType(), setExceptionHandler, setEndHandler, setStreamToFile)

        fn(action)
          .map(p.success)
          .recoverWith({
            case e =>
              logger.error("Upload failed", e)
              p.failure(e)
              Future.failed(e)
          })
      }
    })

    p.future
  }
}
