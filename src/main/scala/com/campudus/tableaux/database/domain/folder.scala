package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import io.vertx.core.json.JsonObject
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

import org.joda.time.DateTime

case class Folder(
    id: FolderId,
    name: String,
    description: String,
    parentIds: Seq[FolderId],
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "id" -> (id match {
      case 0 => None.orNull
      case _ => id
    }),
    "name" -> name,
    "description" -> description,
    "parentId" -> parentIds.lastOption.orNull, // for compatibility
    "parentIds" -> compatibilityGet(parentIds),
    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )
}

case class ExtendedFolder(
    folder: Folder,
    parents: Seq[Folder],
    subfolders: Seq[Folder],
    files: Seq[ExtendedFile]
)(implicit roleModel: RoleModel, user: TableauxUser) extends DomainObject {

  override def getJson: JsonObject = {
    val folderJson = folder.getJson

    val extendedFolderJson = folderJson
      .mergeIn(
        Json.obj(
          "parents" -> compatibilityGet(parents),
          "subfolders" -> compatibilityGet(subfolders),
          "files" -> compatibilityGet(files)
        )
      )

    extendedFolderJson
  }
}
