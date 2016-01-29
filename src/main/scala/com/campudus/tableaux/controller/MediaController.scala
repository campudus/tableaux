package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.{UnknownServerException, CustomException, InvalidRequestException, TableauxConfig}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.model.{FileModel, FolderModel}
import com.campudus.tableaux.router.UploadAction
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.FutureHelper._

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

object MediaController {
  def apply(config: TableauxConfig, folderModel: FolderModel, fileModel: FileModel): MediaController = {
    new MediaController(config, folderModel, fileModel)
  }
}

class MediaController(override val config: TableauxConfig,
                      override protected val repository: FolderModel,
                      protected val fileModel: FileModel) extends Controller[FolderModel] {

  lazy val uploadsDirectory = config.uploadsDirectoryPath()

  /**
    * Alias for None which represents
    * the root folder (which doesn't
    * really exist)
    */
  val rootFolder = Folder(None, "root", "", None, None, None)

  def retrieveFolder(id: FolderId, sortByLangtag: String): Future[ExtendedFolder] = {
    for {
      folder <- repository.retrieve(id)
      extended <- retrieveExtendedFolder(folder, sortByLangtag)
    } yield extended
  }

  def retrieveRootFolder(sortByLangtag: String): Future[ExtendedFolder] = {
    retrieveExtendedFolder(rootFolder, sortByLangtag)
  }

  private def retrieveExtendedFolder(folder: Folder, sortByLangtag: String): Future[ExtendedFolder] = {
    for {
      subfolders <- repository.retrieveSubfolders(folder.id)
      files <- fileModel.retrieveFromFolder(folder.id, sortByLangtag)
    } yield ExtendedFolder(folder, subfolders, files map ExtendedFile)
  }

  def addNewFolder(name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    repository.add(Folder(name, description, parent))
  }

  def changeFolder(id: FolderId, name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    repository.update(Folder(Some(id), name, description, parent, None, None))
  }

  def deleteFolder(id: FolderId): Future[Folder] = {
    for {
      folder <- repository.retrieve(id)

      // delete files & subfolders
      _ <- deleteFilesOfFolder(id).zip(deleteSubfolders(id))

      // delete the folder finally
      _ <- repository.delete(folder)
    } yield folder
  }

  private def deleteSubfolders(id: FolderId): Future[Unit] = {
    for {
      folders <- repository.retrieveSubfolders(Some(id))
      _ <- Future.sequence(folders.map(f => deleteFolder(f.id.get)))
    } yield ()
  }

  private def deleteFilesOfFolder(id: FolderId): Future[Unit] = {
    for {
      files <- fileModel.retrieveFromFolder(Some(id))
      _ <- Future.sequence(files.map(f => deleteFile(f.uuid.get)))
    } yield ()
  }

  def addFile(title: MultiLanguageValue[String], description: MultiLanguageValue[String], externalName: MultiLanguageValue[String], folder: Option[FolderId]): Future[TemporaryFile] = {
    val file = File(UUID.randomUUID(), title, description, externalName, folder)
    fileModel.add(file).map(TemporaryFile)
  }

  def replaceFile(uuid: UUID, langtag: String, upload: UploadAction): Future[ExtendedFile] = futurify { p: Promise[ExtendedFile] =>
    val ext = Path(upload.fileName).extension
    val filePath = uploadsDirectory / Path(s"${UUID.randomUUID()}.$ext")

    upload.exceptionHandler({ ex: Throwable =>
      logger.warn(s"File upload for ${upload.fileName} into ${filePath.name} failed.", ex)

      vertx.fileSystem().delete(filePath.toString(), {
        case Success(_) | Failure(_) => p.failure(ex)
      }: Try[Void] => Unit)
    })

    upload.endHandler({ () =>
      logger.info(s"Uploading of file ${upload.fileName} into ${filePath.name} done, making database entry.")

      val internalName = MultiLanguageValue(Map(langtag -> filePath.name))
      val externalName = MultiLanguageValue(Map(langtag -> upload.fileName))
      val mimeType = MultiLanguageValue(Map(langtag -> upload.mimeType))

      (for {
        (oldFile, paths) <- {
          logger.info("retrieve file")
          retrieveFile(uuid, withTmp = true)
        }

        file = oldFile.file.copy(internalName = internalName, externalName = externalName, mimeType = mimeType)
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
          logger.info(s"update file! $file")
          fileModel.update(file).map(ExtendedFile)
        }
      } yield {
        p.success(updatedFile)
      }) recover {
        case ex =>
          logger.error("Making database entry failed.", ex)

          vertx.fileSystem().delete(filePath.toString(), {
            case Success(_) | Failure(_) => p.failure(ex)
          }: Try[Void] => Unit)
      }
    })

    upload.streamToFile(filePath.toString())
  }

  def changeFile(uuid: UUID, title: MultiLanguageValue[String], description: MultiLanguageValue[String], externalName: MultiLanguageValue[String], internalName: MultiLanguageValue[String], mimeType: MultiLanguageValue[String], folder: Option[FolderId]): Future[ExtendedFile] = {
    def checkInternalName(internalName: String): Future[Unit] = {
      val p: Promise[Unit] = Promise()
      vertx.fileSystem().exists((uploadsDirectory / Path(internalName)).toString(), {
        case Success(java.lang.Boolean.TRUE) => p.success(())
        case Success(java.lang.Boolean.FALSE) => p.failure(InvalidRequestException(s"File with internal name $internalName does not exist"))
        case Failure(ex) => p.failure(ex)
        case _ => p.failure(UnknownServerException("Error in vertx filesystem exists check"))
      }: Try[java.lang.Boolean] => Unit)

      p.future
    }

    Future.sequence(internalName.values.map { x =>
      val internalFileName = x._2
      if (internalFileName == null) {
        Future.successful(())
      } else {
        if (!internalFileName.split("[/\\\\]")(0).equals(internalFileName) ||
          internalFileName.equals("..") ||
          internalFileName.equals(".")) {
          Future.failed(InvalidRequestException(s"Internal name '$internalFileName' is not allowed. Must be the name of a uploaded file with the format: <UUID>.<EXTENSION>."))
        } else {
          checkInternalName(internalFileName)
        }
      }
    }).flatMap { _ =>
      fileModel.update(File(uuid, title, description, externalName, folder, internalName, mimeType)).map(ExtendedFile)
    }
  }

  def retrieveFile(uuid: UUID, withTmp: Boolean = false): Future[(ExtendedFile, Map[String, Path])] = {
    fileModel.retrieve(uuid, withTmp) map { f =>
      val filePaths = f.internalName.values.filter({
        case (_, internalName) =>
          internalName != null && internalName.nonEmpty
      }).map({
        case (langtag, internalName) =>
          langtag -> uploadsDirectory / Path(internalName)
      })

      (ExtendedFile(f), filePaths)
    }
  }

  def deleteFile(uuid: UUID): Future[File] = {
    for {
      (file, paths) <- retrieveFile(uuid, withTmp = true)
      _ <- fileModel.deleteById(uuid)
      _ <- Future.sequence(paths.toSeq.map({
        case (_, path) =>
          deleteFile(path)
      }))
    } yield {
      // return only File not
      // ExtendedFile because url will
      // be invalid after response
      file.file
    }
  }

  def deleteFile(uuid: UUID, langtag: String): Future[File] = {
    for {
      (file, paths) <- retrieveFile(uuid, withTmp = true)
      _ <- fileModel.deleteByIdAndLangtag(uuid, langtag)

      path = paths.get(langtag)

      _ <- if (path.isDefined) {
        deleteFile(path.get)
      } else {
        Future(())
      }

      (file, _) <- retrieveFile(uuid, withTmp = true)
    } yield {
      // return only File not
      // ExtendedFile because url will
      // be invalid after response
      file.file
    }
  }

  private def deleteFile(path: Path): Future[Unit] = {
    import io.vertx.scala.FunctionConverters._

    futurify({ p: Promise[Unit] =>
      val deleteFuture = vertx.fileSystem().delete(path.toString(), _: AsyncVoid)
      deleteFuture.onComplete({
        case Success(_) => p.success(())
        case Failure(e) =>
          val existsFuture = vertx.fileSystem().exists(path.toString(), _: AsyncValue[java.lang.Boolean])
          existsFuture.onComplete({
            case Success(r) =>
              if (r) {
                logger.warn(s"Couldn't delete uploaded file $path: ${e.toString}")
                p.failure(e)
              } else {
                // succeed even if file doesn't exist
                p.success(())
              }
            case Failure(e) =>
              logger.warn("Couldn't check if uploaded file has been deleted.")
              p.failure(e)
          })
      })
    })
  }

  def mergeFile(uuid: UUID, langtag: String, mergeWith: UUID): Future[ExtendedFile] = {
    for {
      toMerge <- fileModel.retrieve(mergeWith)
      file <- fileModel.retrieve(uuid)

      // only delete database entry but not file
      _ <- fileModel.deleteById(mergeWith)

      mergedFile <- {
        val mergeLangtag = isSingleLanguage(toMerge) match {
          case Some(l) => l
          case None => throw new IllegalArgumentException("Can't merge a multi language file.")
        }

        val externalName = toMerge.externalName.get(mergeLangtag)
        val internalName = toMerge.internalName.get(mergeLangtag)

        val title = toMerge.title.get(mergeLangtag)
        val description = toMerge.description.get(mergeLangtag)

        val mimeType = toMerge.mimeType.get(mergeLangtag)

        val mergedFile = file.copy(
          externalName = file.externalName.add(langtag, externalName.getOrElse("")),
          internalName = file.internalName.add(langtag, internalName.getOrElse("")),
          title = file.title.add(langtag, title.getOrElse("")),
          description = file.description.add(langtag, description.getOrElse("")),
          mimeType = file.mimeType.add(langtag, mimeType.getOrElse(""))
        )

        fileModel.update(mergedFile)
      }
    } yield ExtendedFile(mergedFile)
  }

  private def isSingleLanguage(file: File): Option[String] = {
    file.internalName.size == 1 match {
      case true => Some(file.internalName.values.head._1)
      case false => None
    }
  }
}
