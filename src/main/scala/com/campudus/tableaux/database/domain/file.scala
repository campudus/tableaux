package com.campudus.tableaux.database.domain

import java.net.URLEncoder
import java.util.UUID

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

case class TableauxFile(uuid: UUID,

                        folders: Seq[FolderId],

                        title: MultiLanguageValue[String],
                        description: MultiLanguageValue[String],

                        internalName: MultiLanguageValue[String],
                        externalName: MultiLanguageValue[String],

                        mimeType: MultiLanguageValue[String],

                        createdAt: Option[DateTime],
                        updatedAt: Option[DateTime]) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "uuid" -> uuid.toString,

    "folder" -> folders.headOption.orNull,
    "folders" -> compatibilityGet(folders),

    "title" -> title.getJson,
    "description" -> description.getJson,

    "internalName" -> internalName.getJson,
    "externalName" -> externalName.getJson,

    "mimeType" -> mimeType.getJson,

    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )
}

case class TemporaryFile(file: TableauxFile) extends DomainObject {

  override def getJson: JsonObject = Json.obj("tmp" -> true).mergeIn(ExtendedFile(file).getJson)
}

case class ExtendedFile(file: TableauxFile) extends DomainObject {

  override def getJson: JsonObject = Json.obj("url" -> getUrl.getJson).mergeIn(file.getJson)

  private def getUrl: MultiLanguageValue[String] = {
    val urls = file.externalName.values.map({
      case (langtag, filename) =>
        if (filename == null || filename.isEmpty) {
          (langtag, null)
        } else {
          val encodedFilename = URLEncoder.encode(filename, "UTF-8")
          (langtag, s"/files/${file.uuid}/$langtag/$encodedFilename")
        }
    })

    MultiLanguageValue[String](urls)
  }
}