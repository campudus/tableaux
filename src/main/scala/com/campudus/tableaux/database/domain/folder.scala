package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.FolderModel.FolderId
import com.campudus.tableaux.router.auth.permission.{RoleModel, ScopeMedia, TableauxUser}
import io.vertx.core.json.JsonObject
import org.joda.time.DateTime
import org.vertx.scala.core.json._
import io.vertx.scala.ext.web.RoutingContext

case class Folder(
    id: FolderId,
    name: String,
    description: String,
    parents: Seq[FolderId],
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
    "parent" -> parents.lastOption.orNull, // for compatibility
    "parents" -> compatibilityGet(parents),
    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )
}

case class ExtendedFolder(
    folder: Folder,
    subfolders: Seq[Folder],
    files: Seq[ExtendedFile]
)(implicit roleModel: RoleModel, user: TableauxUser) extends DomainObject {

  override def getJson: JsonObject = {
    val folderJson = folder.getJson

    val extendedFolderJson = folderJson
      .mergeIn(
        Json.obj(
          "subfolders" -> compatibilityGet(subfolders),
          "files" -> compatibilityGet(files)
        )
      )

    roleModel.enrichDomainObject(extendedFolderJson, ScopeMedia)
  }
}
