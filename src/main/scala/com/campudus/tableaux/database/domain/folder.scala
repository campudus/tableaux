package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

case class Folder(id: FolderId,
                  name: String,
                  description: String,
                  parents: Seq[FolderId],
                  createdAt: Option[DateTime],
                  updatedAt: Option[DateTime]) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "id" -> id,
    "name" -> name,
    "description" -> description,
    "parent" -> parents.headOption.orNull, // for compatibility
    "parents" -> compatibilityGet(parents),
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