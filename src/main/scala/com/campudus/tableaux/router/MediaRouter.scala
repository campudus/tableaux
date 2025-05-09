package com.campudus.tableaux.router

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.MediaController
import com.campudus.tableaux.database.domain.{DomainObject, MultiLanguageValue}
import com.campudus.tableaux.helper.{AsyncReply, Error, Header, OkBuffer, SendFile}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.router.auth.permission.TableauxUser

import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.http.{HttpServerFileUpload, HttpServerRequest}
import io.vertx.scala.ext.web.{Router, RoutingContext}
import io.vertx.scala.ext.web.handler.BodyHandler
import org.vertx.scala.core.json.Json

import scala.concurrent.{Future, Promise}

import java.awt.image.BufferedImage
import java.io.{ByteArrayOutputStream, File, IOException}
import java.util.UUID

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

  protected val folderId = """(?<folderId>[\d]+)"""

  private val folder: String = s"/folders/$folderId"
  private val folders: String = s"/folders"

  private val file: String = s"/files/$uuidRegex"
  private val fileMerge: String = s"/files/$uuidRegex/merge"
  private val fileDependentRows: String = s"/files/$uuidRegex/dependent"
  private val fileLang: String = s"/files/$uuidRegex/$langtagRegex"
  private val fileLangStatic: String = s"/files/$uuidRegex/$langtagRegex/.*"

  def route: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    router.get(folders).handler(retrieveRootFolder)
    router.getWithRegex(folder).handler(retrieveFolder)
    router.getWithRegex(file).handler(retrieveFile)
    router.getWithRegex(fileDependentRows).handler(retrieveFileDependentRows)
    if (!config.isPublicFileServerEnabled) {
      router.getWithRegex(fileLangStatic).handler(serveFile)
    }

    router.deleteWithRegex(folder).handler(deleteFolder)
    router.deleteWithRegex(file).handler(deleteFile)
    router.deleteWithRegex(fileLang).handler(deleteFileLang)

    // route for file uploading doesn't need a handler yet
    // TODO change to BodyHandler uploading
    // router.put("/files/*").handler(BodyHandler.create().setUploadsDirectory(uploadsDirectory.path))
    // all following routes may require Json in the request body
    val bodyHandler = BodyHandler.create()
    router.putWithRegex(file).handler(bodyHandler)
    router.post("/files/*").handler(bodyHandler)
    router.put("/folders/*").handler(bodyHandler)
    router.post("/folders/*").handler(bodyHandler)

    router.putWithRegex(fileLang).handler(uploadFile)

    router.post(folders).handler(createFolder)
    router.putWithRegex(folder).handler(updateFolder)
    router.post("/files").handler(createFile)
    router.postWithRegex(fileMerge).handler(mergeFile)
    router.putWithRegex(file).handler(updateFile)

    router
  }

  def publicRoute: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    if (config.isPublicFileServerEnabled) {
      router.getWithRegex(fileLangStatic).handler(serveFile)
    }
    router
  }

  private def getFolderId(context: RoutingContext): Option[Long] = {
    implicit val user = TableauxUser(context)
    getLongParam("folderId", context)
  }

  private def createFolder(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)
        val name = json.getString("name")
        val description = json.getString("description")
        val parentId = getNullableLong("parentId")(json)
        controller.addNewFolder(name, description, parentId)
      }
    )
  }

  private def retrieveRootFolder(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(
      context,
      asyncGetReply {
        val sortByLangtag = checked(isDefined(getStringParam("langtag", context), "langtag"))
        controller.retrieveRootFolder(sortByLangtag)
      }
    )
  }

  private def retrieveFolder(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
    for {
      folderId <- getFolderId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val name = json.getString("name")
          val description = json.getString("description")
          val parentId = getNullableLong("parentId")(json)
          controller.changeFolder(folderId, name, description, parentId)
        }
      )
    }
  }

  /**
    * Delete folder and its files
    */
  private def deleteFolder(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
    for {
      fileUuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveFile(UUID.fromString(fileUuid)).map({ case (file, _) => file })
        }
      )
    }
  }

  /**
    * Retrieve file dependent rows
    */
  private def retrieveFileDependentRows(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      fileUuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveFileDependentRows(UUID.fromString(fileUuid))
        }
      )
    }
  }

  private def serveFile(context: RoutingContext): Unit = {
    val isWorkingDirectoryAbsolute = config.isWorkingDirectoryAbsolute
    val thumbnailWidth = getIntQuery("width", context)

    for {
      uuid <- getUUID(context)
      fileUUid = UUID.fromString(uuid)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        AsyncReply {
          for {
            (file, filePaths) <- controller.retrieveFile(fileUUid)
            path <- thumbnailWidth match {
              case Some(width) => controller.retrieveThumbnailPath(fileUUid, langtag, width)
              case _ => Future.successful(filePaths(langtag))
            }
            mimeType = thumbnailWidth match {
              case Some(width) => "image/png"
              case _ => file.mimeType.get(langtag).getOrElse("")
            }
          } yield {
            Header("Content-type", mimeType, SendFile(path.toString(), isWorkingDirectoryAbsolute))
          }
        }
      )
    }
  }

  /**
    * Update file meta information
    */
  private def updateFile(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
    for {
      fileUuid <- getUUID(context)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          handleUpload(
            context,
            (action: UploadAction) => controller.replaceFile(UUID.fromString(fileUuid), langtag, action)
          )
        }
      )
    }
  }

  private def mergeFile(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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

  private def handleUpload(
      implicit context: RoutingContext,
      fn: UploadAction => Future[DomainObject]
  ): Future[DomainObject] = {

    val req: HttpServerRequest = context.request().setExpectMultipart(true)

    logger.info(s"Handle upload for ${req.absoluteURI()}")

    val p = Promise[DomainObject]()

    val timerId = vertx.setTimer(
      10000L,
      _ => {
        p.failure(
          RouterException(
            message = "No valid file upload received",
            id = "errors.upload.invalidRequest",
            statusCode = 400
          )
        )
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
