package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.model.{FileModel, FolderModel}
import com.campudus.tableaux.helper.FutureUtils
import com.campudus.tableaux.router.UploadAction

import scala.concurrent.{Promise, Future}
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

  def retrieveFolder(id: FolderId): Future[ExtendedFolder] = {
    for {
      folder <- repository.retrieve(id)
      subfolders <- repository.retrieveSubfolders(id)
      files <- fileModel.retrieveFromFolder(id)
    } yield ExtendedFolder(folder, subfolders, files)
  }

  def addNewFolder(name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    repository.add(Folder(name, description, parent))
  }

  def changeFolder(id: FolderId, name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    repository.update(Folder(Some(id), name, description, parent, None, None))
  }

  def uploadFile(upload: UploadAction): Future[TemporaryFile] = promisify { p: Promise[TemporaryFile] =>
    val uuid = UUID.randomUUID()
    val ext = Path(upload.fileName).extension

    val filePath = uploadsDirectory / Path(s"$uuid.$ext")

    // TODO perhaps I should use vertx.filesystem here
    filePath.parent.createDirectory()

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
      val path = uploadsDirectory / Path(s"${f.uuid.get}.$ext")
      (ExtendedFile(f), path)
    }
  }

  def deleteFile(uuid: UUID): Future[File] = {
    for {
      (file, path) <- retrieveFile(uuid)
      _ <- fileModel.deleteById(uuid)
      _ <- {
        import FutureUtils._
        promisify({ p: Promise[Unit] =>
          vertx.fileSystem.delete(path.toString(), {result => p.success()})
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
