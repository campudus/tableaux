package com.campudus.tableaux.database.domain

import java.util.UUID

import com.campudus.tableaux.database.model.FolderModel.FolderId
import org.joda.time.DateTime
import org.vertx.scala.core.json._

object File {
  def apply(name: String, mimeType: String, description: String): File = {
    File(None, name, description, mimeType, name, None, None, None)
  }

  def apply(uuid: UUID, name: String, mimeType: String, description: String): File = {
    File(Option(uuid), name, description, mimeType, name, None, None, None)
  }

  def apply(uuid: UUID, name: String, mimeType: String): File = {
    File(Option(uuid), name, null, mimeType, name, None, None, None)
  }
}

case class File(uuid: Option[UUID],
                name: String,
                description: String,
                mimeType: String,
                filename: String,
                folder: Option[FolderId],
                createdAt: Option[DateTime],
                updatedAt: Option[DateTime]) extends DomainObject {

  override def getJson: JsonObject = Json.obj(
    "uuid" -> optionToString(uuid),
    "name" -> name,
    "description" -> description,
    "mimeType" -> mimeType,
    "filename" -> filename,
    "folder" -> optionToString(folder),
    "createdAt" -> optionToString(createdAt),
    "updatedAt" -> optionToString(updatedAt)
  )

  override def setJson: JsonObject = getJson

  def optionToString[A](option: Option[A]): String = {
    option.map(_.toString).orNull
  }
}

case class TemporaryFile(file: File) extends DomainObject {

  override def getJson: JsonObject = Json.obj("tmp" -> true).mergeIn(file.getJson)

  override def setJson: JsonObject = getJson
}

case class ExtendedFile(file: File) extends DomainObject {

  override def getJson: JsonObject = Json.obj("url" -> s"/files/${file.uuid.get}/${file.filename}").mergeIn(file.getJson)

  override def setJson: JsonObject = getJson
}