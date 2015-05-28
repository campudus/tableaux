package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain.File
import com.campudus.tableaux.database.model.FileModel
import com.campudus.tableaux.helper.FutureUtils
import org.joda.time.DateTime

import scala.concurrent.{Promise, Future}

sealed trait FileAction

case class UploadAction(fileName: String,
                        mimeType: String,
                        exceptionHandler: (Throwable => Unit) => _,
                        endHandler: (() => Unit) => _,
                        streamToFile: (String) => _) extends FileAction

object FileController {
  def apply(config: TableauxConfig, repository: FileModel): FileController = {
    new FileController(config, repository)
  }
}

class FileController(override val config: TableauxConfig,
                     override protected val repository: FileModel) extends Controller[FileModel] {
  import FutureUtils._

  def uploadFile(upload: UploadAction): Future[File] = promisify { p: Promise[File] =>
    val newFileId = UUID.randomUUID()
    val fileName = s"${config.workingDirectory}/${config.uploadDirectory}/$newFileId"

    upload.exceptionHandler({ ex: Throwable =>
      logger.warn(s"File upload for ${upload.fileName} into $newFileId failed.", ex)
      p.failure(ex)
    })

    upload.endHandler({ () =>
      logger.info(s"Uploading of file ${upload.fileName} into $newFileId done, making database entry.")
      repository.add(File(upload.fileName, upload.mimeType, ""))
    })

    upload.streamToFile(fileName)
  }
}
