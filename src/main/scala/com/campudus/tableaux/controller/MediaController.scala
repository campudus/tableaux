package com.campudus.tableaux.controller

import com.campudus.tableaux.{InvalidRequestException, TableauxConfig, UnknownServerException}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.{AttachmentModel, FileModel, FolderModel, StructureModel, TableauxModel}
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.router.RouterException
import com.campudus.tableaux.router.UploadAction
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.verticles.EventClient

import io.vertx.core.eventbus.{ReplyException, ReplyFailure}
import io.vertx.scala.FutureHelper._
import io.vertx.scala.ext.web.RoutingContext

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Success}

import java.util.UUID

object MediaController {

  /**
    * Alias for None which represents the root folder (which doesn't really exist)
    */
  val ROOT_FOLDER = Folder(0, "root", "", Seq.empty[FolderId], None, None)

  def apply(
      config: TableauxConfig,
      folderModel: FolderModel,
      fileModel: FileModel,
      attachmentModel: AttachmentModel,
      roleModel: RoleModel,
      tableauxModel: TableauxModel
  ): MediaController = {
    new MediaController(config, folderModel, fileModel, attachmentModel, roleModel, tableauxModel)
  }
}

class MediaController(
    override val config: TableauxConfig,
    override protected val repository: FolderModel,
    protected val fileModel: FileModel,
    protected val attachmentModel: AttachmentModel,
    implicit protected val roleModel: RoleModel,
    protected val tableauxModel: TableauxModel
) extends Controller[FolderModel] {

  import MediaController.ROOT_FOLDER

  val eventClient: EventClient = EventClient(vertx)

  lazy val uploadsDirectory: Path = config.uploadsDirectoryPath()

  def retrieveFolder(id: FolderId, sortByLangtag: String)(
      implicit user: TableauxUser
  ): Future[ExtendedFolder] = {
    for {
      folder <- repository.retrieve(id)
      extended <- retrieveExtendedFolder(folder, sortByLangtag)
    } yield extended
  }

  def retrieveRootFolder(sortByLangtag: String)(implicit user: TableauxUser): Future[ExtendedFolder] = {
    retrieveExtendedFolder(ROOT_FOLDER, sortByLangtag)
  }

  private def retrieveExtendedFolder(folder: Folder, sortByLangtag: String)(
      implicit user: TableauxUser
  ): Future[ExtendedFolder] = {
    for {
      parents <- repository.retrieveParentfolders(folder)
      subfolders <- repository.retrieveSubfolders(folder)
      files <- fileModel.retrieveFromFolder(folder, sortByLangtag)
    } yield ExtendedFolder(folder, parents, subfolders, files.map(ExtendedFile))
  }

  def addNewFolder(name: String, description: String, parentId: Option[FolderId])(
      implicit user: TableauxUser
  ): Future[Folder] = {
    for {
      _ <- roleModel.checkAuthorization(CreateMedia)
      folder <- repository.add(name, description, parentId)
    } yield folder
  }

  def changeFolder(id: FolderId, name: String, description: String, parentId: Option[FolderId])(
      implicit user: TableauxUser
  ): Future[Folder] = {
    for {
      _ <- roleModel.checkAuthorization(EditMedia)
      folder <- repository.update(id, name, description, parentId)
    } yield folder
  }

  def deleteFolder(id: FolderId)(implicit user: TableauxUser): Future[Folder] = {
    for {
      _ <- roleModel.checkAuthorization(DeleteMedia)
      folder <- repository.retrieve(id)

      // delete files
      files <- fileModel.retrieveFromFolder(folder)
      _ <- Future.sequence(files.map(f => deleteFile(f.uuid)))

      // delete subfolders
      folders <- repository.retrieveSubfolders(folder)
      _ <- Future.sequence(folders.map(f => deleteFolder(f.id)))

      // delete the folder finally
      _ <- repository.delete(folder)
    } yield folder
  }

  def addFile(
      title: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      externalName: MultiLanguageValue[String],
      folder: Option[FolderId]
  )(implicit user: TableauxUser): Future[TemporaryFile] = {
    for {
      _ <- roleModel.checkAuthorization(CreateMedia)
      file <- fileModel.add(title, description, externalName, folder).map(TemporaryFile)
    } yield file
  }

  def replaceFile(uuid: UUID, langtag: String, upload: UploadAction)(
      implicit user: TableauxUser
  ): Future[ExtendedFile] = {
    futurify { p: Promise[ExtendedFile] =>
      {
        val ext = Path(upload.fileName).extension
        val filePath = uploadsDirectory / Path(s"${UUID.randomUUID()}.$ext")

        upload.exceptionHandler({ ex: Throwable =>
          logger.warn(s"File upload for ${upload.fileName} into ${filePath.name} failed.", ex)

          vertx
            .fileSystem()
            .deleteFuture(filePath.toString())
            .onComplete({
              case Success(_) | Failure(_) => p.failure(ex)
            })
        })

        upload.endHandler({ () =>
          logger.info(s"Uploading of file ${upload.fileName} into ${filePath.name} done, making database entry.")

          val internalName = MultiLanguageValue(Map(langtag -> filePath.name))
          val externalName = MultiLanguageValue(Map(langtag -> upload.fileName))
          val mimeType = MultiLanguageValue(Map(langtag -> upload.mimeType))

          (for {
            _ <- roleModel.checkAuthorization(EditMedia)

            (oldFile, paths) <- {
              logger.info("retrieve file")
              retrieveFile(uuid, withTmp = true)
            }

            path = paths.get(langtag)

            _ <- {
              logger.info(s"delete old file $path")
              if (path.isDefined) {
                deleteFile(path.get)
              } else {
                Future.successful(())
              }
            }

            updatedFile <- {
              fileModel
                .update(
                  uuid = oldFile.file.uuid,
                  title = oldFile.file.title,
                  description = oldFile.file.description,
                  internalName = internalName,
                  externalName = externalName,
                  folder = oldFile.file.folders.lastOption,
                  mimeType = mimeType
                )
                .map(ExtendedFile)
            }

            // invalidate cdn cache for old file
            _ <- eventClient.fileChanged(oldFile)

            // retrieve cells with this file for cache invalidation
            cellsForFiles <- attachmentModel.retrieveCells(uuid)
            // invalidate cache for cells with this file
            _ <- Future.sequence(cellsForFiles.map({
              case (tableId, columnId, rowId) => eventClient.invalidateCellValue(tableId, columnId, rowId)
            }))
          } yield {
            p.success(updatedFile)
          }) recover {
            case ex =>
              logger.error("Making database entry failed.", ex)

              vertx
                .fileSystem()
                .deleteFuture(filePath.toString())
                .onComplete({
                  case Success(_) | Failure(_) => p.failure(ex)
                })
          }
        })

        upload.streamToFile(filePath.toString())
      }
    }
  }

  def changeFile(
      uuid: UUID,
      title: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      externalName: MultiLanguageValue[String],
      internalName: MultiLanguageValue[String],
      mimeType: MultiLanguageValue[String],
      folder: Option[FolderId]
  )(implicit user: TableauxUser): Future[ExtendedFile] = {

    def checkInternalName(internalName: String): Future[Unit] = {
      vertx
        .fileSystem()
        .existsFuture((uploadsDirectory / Path(internalName)).toString())
        .recover({
          case ex => UnknownServerException("Error in vertx filesystem exists check", ex)
        })
        .flatMap({
          case true => Future.successful(())
          case false => Future.failed(InvalidRequestException(s"File with internal name $internalName does not exist"))
        })
    }

    val internalNameChecks = internalName.values
      .mapValues(Option.apply)
      .map({
        case (_, None) =>
          Future.successful(())
        case (_, Some(internalFileName)) =>
          if (
            !internalFileName.split("[/\\\\]")(0).equals(internalFileName) ||
            internalFileName.equals("..") ||
            internalFileName.equals(".")
          ) {
            Future.failed(InvalidRequestException(
              s"Internal name '$internalFileName' is not allowed. Must be the name of a uploaded file with the format: <UUID>.<EXTENSION>."
            ))
          } else {
            checkInternalName(internalFileName)
          }
      })

    for {
      _ <- roleModel.checkAuthorization(EditMedia)
      _ <- Future.sequence(internalNameChecks)

      file <- fileModel.update(uuid, title, description, internalName, externalName, folder, mimeType)

      // retrieve cells with this file for cache invalidation
      cellsForFiles <- attachmentModel.retrieveCells(uuid)
      // invalidate cache for cells with this file
      _ <- Future.sequence(cellsForFiles.map({
        case (tableId, columnId, rowId) => eventClient.invalidateCellValue(tableId, columnId, rowId)
      }))
    } yield ExtendedFile(file)
  }

  def retrieveFile(uuid: UUID, withTmp: Boolean = false): Future[(ExtendedFile, Map[String, Path])] = {
    fileModel.retrieve(uuid, withTmp) map { f =>
      {
        val filePaths = f.internalName.values
          .filter({
            case (_, internalName) =>
              Option(internalName).isDefined && internalName.nonEmpty
          })
          .map({
            case (langtag, internalName) =>
              langtag -> uploadsDirectory / Path(internalName)
          })

        (ExtendedFile(f), filePaths)
      }
    }
  }

  def retrieveThumbnailPath(uuid: UUID, langtag: String, width: Int): Future[Path] = {
    val timeout = Option(config.thumbnailsConfig.getInteger("timeout")).map(_.intValue).getOrElse(10000);
    val context = s"(uuid: ${uuid.toString}, langtag: $langtag, width: ${width.toString})"

    width match {
      case w if w <= 0 =>
        logger.error(s"Invalid thumbnail request $context: width must be positive")
        Future.failed(InvalidRequestException(s"Invalid thumbnail request: width must be positive"))
      case _ => eventClient.retrieveThumbnailPath(uuid, langtag, width, timeout).recoverWith({
          case ex: ReplyException if ex.failureCode() == 400 =>
            logger.error(s"Invalid thumbnail request $context: ${ex.getMessage}", ex)
            Future.failed(InvalidRequestException(s"Invalid thumbnail request: ${ex.getMessage}"))

          case ex: ReplyException if ex.failureType() == ReplyFailure.TIMEOUT =>
            logger.error(s"Thumbnail request timed out $context: ${ex.getMessage}", ex)
            Future.failed(UnknownServerException("Thumbnail request timed out"))

          case ex: Throwable =>
            logger.error(s"Unexpected error retrieving thumbnail $context: ${ex.getMessage}", ex)
            Future.failed(UnknownServerException("Unexpected error retrieving thumbnail"))
        })
    }
  }

  def deleteFile(uuid: UUID)(implicit user: TableauxUser): Future[TableauxFile] = {
    for {
      _ <- roleModel.checkAuthorization(DeleteMedia)

      (file, paths) <- retrieveFile(uuid, withTmp = true)

      // retrieve cells with this file for cache invalidation
      cellsForFiles <- attachmentModel.retrieveCells(uuid)

      _ <- fileModel.deleteById(uuid)

      _ <- Future.sequence(paths.toSeq.map({
        case (_, path) =>
          deleteFile(path)
      }))

      // invalidate cache for cells with this file
      _ <- Future.sequence(cellsForFiles.map({
        case (tableId, columnId, rowId) => eventClient.invalidateCellValue(tableId, columnId, rowId)
      }))
    } yield {
      // return only File not
      // ExtendedFile because url will
      // be invalid after response
      file.file
    }
  }

  def deleteFile(uuid: UUID, langtag: String)(implicit user: TableauxUser): Future[TableauxFile] = {
    for {
      _ <- roleModel.checkAuthorization(DeleteMedia)

      (_, paths) <- retrieveFile(uuid, withTmp = true)

      // retrieve cells with this file for cache invalidation
      cellsForFiles <- attachmentModel.retrieveCells(uuid)

      _ <- fileModel.deleteByIdAndLangtag(uuid, langtag)

      path = paths.get(langtag)

      _ <-
        if (path.isDefined) {
          deleteFile(path.get)
        } else {
          Future(())
        }

      // invalidate cache for cells with this file
      _ <- Future.sequence(cellsForFiles.map({
        case (tableId, columnId, rowId) => eventClient.invalidateCellValue(tableId, columnId, rowId)
      }))

      (file, _) <- retrieveFile(uuid, withTmp = true)
    } yield {
      // return only File not
      // ExtendedFile because url will
      // be invalid after response
      file.file
    }
  }

  private def deleteFile(path: Path): Future[Unit] = {
    vertx
      .fileSystem()
      .deleteFuture(path.toString())
      .recoverWith({
        case deleteEx =>
          vertx
            .fileSystem()
            .existsFuture(path.toString())
            .flatMap({
              case true =>
                logger.warn(s"Couldn't delete uploaded file $path: ${deleteEx.toString}")
                Future.failed(deleteEx)
              case false =>
                Future.successful(())
            })
      })
  }

  def mergeFile(uuid: UUID, langtag: String, mergeWith: UUID)(
      implicit user: TableauxUser
  ): Future[ExtendedFile] = {
    for {
      _ <- roleModel.checkAuthorization(EditMedia)
      toMerge <- fileModel.retrieve(mergeWith)
      file <- fileModel.retrieve(uuid)

      // retrieve cells with this file for cache invalidation
      cellsForFile1 <- attachmentModel.retrieveCells(uuid)
      cellsForFile2 <- attachmentModel.retrieveCells(mergeWith)

      // only delete database entry but not file
      _ <- fileModel.deleteById(mergeWith)

      mergedFile <- {
        val mergeLangtag = toMerge.isSingleLanguage match {
          case Some(l) => l
          case None => throw new IllegalArgumentException("Can't merge a multi language file.")
        }

        val externalName = toMerge.externalName.get(mergeLangtag)
        val internalName = toMerge.internalName.get(mergeLangtag)

        val title = toMerge.title.get(mergeLangtag)
        val description = toMerge.description.get(mergeLangtag)

        val mimeType = toMerge.mimeType.get(mergeLangtag)

        fileModel.update(
          uuid = uuid,
          title = file.title.add(langtag, title.getOrElse("")),
          description = file.description.add(langtag, description.getOrElse("")),
          internalName = file.internalName.add(langtag, internalName.getOrElse("")),
          externalName = file.externalName.add(langtag, externalName.getOrElse("")),
          folder = file.folders.lastOption,
          mimeType = file.mimeType.add(langtag, mimeType.getOrElse(""))
        )
      }

      // invalidate cache for cells with this file
      cellsForFiles = cellsForFile1 ++ cellsForFile2
      _ <- Future.sequence(cellsForFiles.map({
        case (tableId, columnId, rowId) => eventClient.invalidateCellValue(tableId, columnId, rowId)
      }))
    } yield ExtendedFile(mergedFile)
  }

  def retrieveFileDependentRows(uuid: UUID)(
      implicit user: TableauxUser
  ): Future[FileDependentRowsSeq] = {
    logger.info(s"retrieveFileDependentRows $uuid")

    tableauxModel.retrieveFileDependentRows(uuid)
  }
}
