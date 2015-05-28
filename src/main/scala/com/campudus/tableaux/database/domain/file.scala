package com.campudus.tableaux.database.domain

import java.util.UUID

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

object File {
  def apply(name: String, mimeType: String, description: String): File = {
    File(None, name, description, mimeType, None, None, None)
  }

  def apply(uuid: UUID, name: String, mimeType: String, description: String): File = {
    File(Option(uuid), name, description, mimeType, None, None, None)
  }
}

case class File(uuid: Option[UUID],
                name: String,
                description: String,
                mimeType: String,
                folder: Option[FolderId],
                createdAt: Option[DateTime],
                updatedAt: Option[DateTime]) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "uuid" -> optionToString(uuid),
    "name" -> name,
    "description" -> description,
    "mimeType" -> mimeType,
    "folder" -> optionToString(folder),
    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )

  override def setJson: JsonObject = getJson

  def optionToString[A](option: Option[A]): String = {
    if (option.isEmpty) {
      null
    } else {
      option.get.toString
    }
  }
}

case class ExtendedFile(file: File,
                        url: String) extends DomainObject {

  override def getJson: JsonObject = Json.obj("url" -> url).mergeIn(file.getJson)

  override def setJson: JsonObject = getJson
}