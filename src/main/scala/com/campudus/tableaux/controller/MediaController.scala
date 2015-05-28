package com.campudus.tableaux.controller

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain.{ExtendedFolder, Folder}
import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.database.model.{FileModel, FolderModel}

import scala.concurrent.Future

object MediaController {
  def apply(config: TableauxConfig, folderModel: FolderModel, fileModel: FileModel): MediaController = {
    new MediaController(config, folderModel, fileModel)
  }
}

class MediaController(override val config: TableauxConfig,
                      override protected val repository: FolderModel,
                      protected val fileModel: FileModel) extends Controller[FolderModel] {

  def retrieveFolder(id: FolderId): Future[ExtendedFolder] = {
    println(s"$id")
    for {
      folder <- repository.retrieve(id)
      subfolders <- repository.retrieveSubfolders(id)
      files <- fileModel.retrieveFromFolder(id)
    } yield ExtendedFolder(folder, subfolders, files)
  }

  def addNewFolder(name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    println(s"$name $description $parent")
    repository.add(Folder(name, description, parent))
  }

  def changeFolder(id: FolderId, name: String, description: String, parent: Option[FolderId]): Future[Folder] = {
    println(s"$id $name $description $parent")
    repository.update(Folder(Some(id), name, description, parent, None, None))
  }
}
