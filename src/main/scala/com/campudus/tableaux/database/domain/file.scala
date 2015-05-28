package com.campudus.tableaux.database.domain

import java.util.UUID

import org.joda.time.DateTime
import org.vertx.scala.core.json._

object File {
  def apply(name: String, mimeType: String, fileType: String): File = {
    File(None, name, mimeType, fileType, None, None)
  }

  def apply(uuid: UUID, name: String, mimeType: String, fileType: String): File = {
    File(Option(uuid), name, mimeType, fileType, None, None)
  }
}

case class File(uuid: Option[UUID],
                name: String,
                mimeType: String,
                fileType: String,
                createdAt: Option[DateTime],
                updatedAt: Option[DateTime]) extends DomainObject {
  override def getJson: JsonObject = Json.obj()

  override def setJson: JsonObject = Json.obj()
}