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

  def uploadFile(upload: UploadAction): Future[TemporaryFile] = promisify { p: Promise[TemporaryFile] =>
    val uuid = UUID.randomUUID()
    val ext = Path(upload.fileName).extension

    val filePath = uploadsDirectory / Path(s"$uuid.$ext")

    upload.exceptionHandler({ ex: Throwable =>
      logger.warn(s"File upload for ${upload.fileName} into ${filePath.name} failed.", ex)
      p.failure(ex)
    })

    upload.endHandler({ () =>
      logger.info(s"Uploading of file ${upload.fileName} into ${filePath.name} done, making database entry.")

      val file = File(uuid, upload.fileName, upload.mimeType)
      val insertedFile = fileModel.add(file)

      insertedFile map { f =>
        p.success(TemporaryFile(f))
      }
    })

    upload.streamToFile(filePath.toString())
  }

  def changeFile(uuid: UUID, name: String, description: String, folder: Option[FolderId]): Future[File] = {
    fileModel.update(File(Some(uuid), name, description, null, null, folder, None, None))
  }

  def retrieveFile(uuid: UUID): Future[(ExtendedFile, Path)] = {
    fileModel.retrieve(uuid) map { f =>
      val ext = Path(f.filename).extension
      val filePath = uploadsDirectory / Path(s"$uuid.$ext")

      (ExtendedFile(f), filePath)
    }
  }

  def deleteFile(uuid: UUID): Future[File] = {
    for {
      (file, path) <- retrieveFile(uuid)
      _ <- fileModel.deleteById(uuid)
      _ <- {
        import FutureUtils._

        promisify({ p: Promise[Unit] =>
          // succeed even if file doesn't exist
          vertx.fileSystem.delete(path.toString(), { result => p.success(()) })
        })
      }
    } yield {
      // return only File not
      // ExtendedFile because url will
      // be invalid after response
      file.file
    }
  }
}
