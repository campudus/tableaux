package com.campudus.tableaux.database.domain

import java.net.URLEncoder
import java.util.UUID

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

case class TableauxFile(
    uuid: UUID,
    folders: Seq[FolderId],
    title: MultiLanguageValue[String],
    description: MultiLanguageValue[String],
    internalName: MultiLanguageValue[String],
    externalName: MultiLanguageValue[String],
    mimeType: MultiLanguageValue[String],
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "uuid" -> uuid.toString,
    "folder" -> folders.lastOption.orNull,
    "folders" -> compatibilityGet(folders),
    "title" -> title.getJson,
    "description" -> description.getJson,
    "internalName" -> internalName.getJson,
    "externalName" -> externalName.getJson,
    "mimeType" -> mimeType.getJson,
    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )

  /**
    * @return None if multi-language and Some('de-DE') if single-language
    */
  def isSingleLanguage: Option[String] = {
    internalName.values.toList match {
      case (langtag, value) :: Nil => Some(langtag)
      case _ => None
    }
  }
}

case class TemporaryFile(file: TableauxFile) extends DomainObject {
  val _file = ExtendedFile(file)

  val uuid: UUID = _file.uuid
  val folders: Seq[FolderId] = _file.folders

  val title: MultiLanguageValue[String] = _file.title
  val description: MultiLanguageValue[String] = _file.description

  val internalName: MultiLanguageValue[String] = _file.internalName
  val externalName: MultiLanguageValue[String] = _file.externalName

  val mimeType: MultiLanguageValue[String] = _file.mimeType

  val createdAt: Option[DateTime] = _file.createdAt
  val updatedAt: Option[DateTime] = _file.updatedAt

  override def getJson: JsonObject = Json.obj("tmp" -> true).mergeIn(_file.getJson)
}

case class ExtendedFile(file: TableauxFile) extends DomainObject {

  val uuid: UUID = file.uuid
  val folders: Seq[FolderId] = file.folders

  val title: MultiLanguageValue[String] = file.title
  val description: MultiLanguageValue[String] = file.description

  val internalName: MultiLanguageValue[String] = file.internalName
  val externalName: MultiLanguageValue[String] = file.externalName

  val mimeType: MultiLanguageValue[String] = file.mimeType

  val createdAt: Option[DateTime] = file.createdAt
  val updatedAt: Option[DateTime] = file.updatedAt

  override def getJson: JsonObject = Json.obj("url" -> getUrl.getJson).mergeIn(file.getJson)

  private def getUrl: MultiLanguageValue[String] = {
    val langtags = (internalName.langtags ++ externalName.langtags).distinct

    val urls = langtags
      .map({ langtag =>
        {
          val filename = externalName.get(langtag).getOrElse(internalName.get(langtag).orNull)
          val encodedFilename = URLEncoder.encode(filename, "UTF-8")
          (langtag, s"/files/${file.uuid}/$langtag/$encodedFilename")
        }
      })
      .toMap

    MultiLanguageValue[String](urls)
  }
}
