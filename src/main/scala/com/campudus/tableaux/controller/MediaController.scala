package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.model.{FileModel, FolderModel}
import com.campudus.tableaux.helper.FutureUtils
import com.campudus.tableaux.router.UploadAction

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

  import FutureUtils._

  lazy val uploadsDirectory = Path(s"${config.workingDirectory}/${config.uploadsDirectory}")

  /**
   * Alias for None which represents
   * the root folder (which doesn't
   * really exist)
   */
  val root = None

  val rootFolder = Folder(None, "root", "", None, None, None)

  def retrieveFolder(id: FolderId): Future[ExtendedFolder] = {
    for {
      folder <- repository.retrieve(id)
      extended <- retrieveExtendedFolder(folder)
    } yield extended
  }

  def retrieveRootFolder(): Future[ExtendedFolder] = {
    retrieveExtendedFolder(rootFolder)
  }

  private def retrieveExtendedFolder(folder: Folder): Future[ExtendedFolder] = {
    for {
      subfolders <- repository.retrieveSubfolders(folder.id)
      files <- fileModel.retrieveFromFolder(folder.id)
      extendedFiles <- Future(files map ExtendedFile)
    } yield ExtendedFolder(folder, subfolders, extendedFiles)
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

  def uploadFile(langtag: String, upload: UploadAction): Future[TemporaryFile] = promisify { p: Promise[TemporaryFile] =>
    val ext = Path(upload.fileName).extension
    val filePath = uploadsDirectory / Path(s"${UUID.randomUUID()}.$ext")

    upload.exceptionHandler({ ex: Throwable =>
      logger.warn(s"File upload for ${upload.fileName} into ${filePath.name} failed.", ex)
      p.failure(ex)
    })

    upload.endHandler({ () =>
      logger.info(s"Uploading of file ${upload.fileName} into ${filePath.name} done, making database entry.")

      val internalName = MultiLanguageValue(Map(langtag -> filePath.name))
      val externalName = MultiLanguageValue(Map(langtag -> upload.fileName))
      val mimeType = MultiLanguageValue(Map(langtag -> upload.mimeType))

      // Generate new uuid for file entry.
      val file = File(UUID.randomUUID(), internalName, externalName, mimeType)
      val insertedFile = fileModel.add(file)

      insertedFile map { f =>
        p.success(TemporaryFile(f))
      }
    })

    upload.streamToFile(filePath.toString())
  }

  def replaceFile(uuid: UUID, langtag: String, upload: UploadAction): Future[File] = promisify { p: Promise[File] =>
    val ext = Path(upload.fileName).extension
    val filePath = uploadsDirectory / Path(s"${UUID.randomUUID()}.$ext")

    upload.exceptionHandler({ ex: Throwable =>
      logger.warn(s"File upload for ${upload.fileName} into ${filePath.name} failed.", ex)
      p.failure(ex)
    })

    upload.endHandler({ () =>
      logger.info(s"Uploading of file ${upload.fileName} into ${filePath.name} done, making database entry.")

      val internalName = MultiLanguageValue(Map(langtag -> filePath.name))
      val externalName = MultiLanguageValue(Map(langtag -> upload.fileName))
      val mimeType = MultiLanguageValue(Map(langtag -> upload.mimeType))

      for {
        (oldFile, paths) <- retrieveFile(uuid)

        file = oldFile.file.copy(internalName = internalName, externalName = externalName, mimeType = mimeType)
        path = paths.get(langtag)

        _ <- if (path.isDefined) {
          deleteFile(path.get)
        } else {
          Future(())
        }

        updatedFile <- fileModel.update(file)
      } yield {
        p.success(updatedFile)
      }
    })

    upload.streamToFile(filePath.toString())
  }

  def changeFile(uuid: UUID, title: MultiLanguageValue[String], description: MultiLanguageValue[String], folder: Option[FolderId]): Future[ExtendedFile] = {
    fileModel.update(File(uuid, title, description, folder)).map(ExtendedFile)
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
    import FutureUtils._
    import org.vertx.scala.core.FunctionConverters._

    promisify({ p: Promise[Unit] =>
      vertx.fileSystem.delete(path.toString(), {
        case Success(v) =>
          p.success(())
        case Failure(e) =>
          vertx.fileSystem.exists(path.toString(), { result =>
            if (result.result()) {
              logger.warn(s"Couldn't delete uploaded file $path: ${e.toString}")
              p.failure(e)
            } else {
              // succeed even if file doesn't exist
              p.success(())
            }
          })

      }: Try[Void] => Unit)
    })
  }
}
