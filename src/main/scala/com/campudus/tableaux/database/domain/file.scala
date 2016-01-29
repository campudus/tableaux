package com.campudus.tableaux.database.domain

import java.net.URLEncoder
import java.util.UUID

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

// TODO Rename "File" to TableauxFile or similar..
object File {
  def apply(uuid: UUID, name: MultiLanguageValue[String], description: MultiLanguageValue[String], externalName: MultiLanguageValue[String], folder: Option[FolderId], internalName: MultiLanguageValue[String] = MultiLanguageValue.empty(), mimeType: MultiLanguageValue[String] = MultiLanguageValue.empty()): File = {
    File(Some(uuid), folder, name, description, internalName, externalName, mimeType, None, None)
  }
}

case class File(uuid: Option[UUID], // TODO Shouldn't a File always have a UUID?
                folder: Option[FolderId],

                title: MultiLanguageValue[String],
                description: MultiLanguageValue[String],

                internalName: MultiLanguageValue[String],
                externalName: MultiLanguageValue[String],

                mimeType: MultiLanguageValue[String],

                createdAt: Option[DateTime],
                updatedAt: Option[DateTime]) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "uuid" -> optionToString(uuid),
    "folder" -> folder.orNull,

    "title" -> title.getJson,
    "description" -> description.getJson,

    "internalName" -> internalName.getJson,
    "externalName" -> externalName.getJson,

    "mimeType" -> mimeType.getJson,

    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )
}

case class TemporaryFile(file: File) extends DomainObject {

  override def getJson: JsonObject = Json.obj("tmp" -> true).mergeIn(ExtendedFile(file).getJson)
}

case class ExtendedFile(file: File) extends DomainObject {

  override def getJson: JsonObject = Json.obj("url" -> getUrl.getJson).mergeIn(file.getJson)

  private def getUrl: MultiLanguageValue[String] = {
    val uuid = file.uuid.get // TODO possible null pointer, if UUID is not set (I guess it should always be set, but why Option[UUID] then?)
    val urls = file.externalName.values.map({
      case (langtag, filename) =>
        if (filename == null || filename.isEmpty) {
          (langtag, null)
        } else {
          val encodedFilename = URLEncoder.encode(filename, "UTF-8")
          (langtag, s"/files/${file.uuid.get}/$langtag/$encodedFilename")
        }
    })

    MultiLanguageValue[String](urls)
  }
}