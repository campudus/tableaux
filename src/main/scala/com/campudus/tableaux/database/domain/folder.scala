package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

object Folder {
  def apply(name: String, description: String, parent: Option[FolderId]): Folder = {
    new Folder(None, name, description, parent, None, None)
  }
}

case class Folder(id: Option[FolderId],
                  name: String,
                  description: String,
                  parent: Option[FolderId],
                  createdAt: Option[DateTime],
                  updatedAt: Option[DateTime]) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "id" -> id.orNull,
    "name" -> name,
    "description" -> description,
    "parent" -> parent.orNull,
    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )
}

case class ExtendedFolder(folder: Folder,
                          subfolders: Seq[Folder],
                          files: Seq[ExtendedFile]) extends DomainObject {
  override def getJson: JsonObject = {
    val folderJson = folder.getJson

    folderJson.mergeIn(Json.obj(
      "subfolders" -> compatibilityGet(subfolders),
      "files" -> compatibilityGet(files)
    ))
  }
}