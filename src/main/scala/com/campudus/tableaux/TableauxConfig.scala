package com.campudus.tableaux

import com.campudus.tableaux.helper.VertxAccess

import io.vertx.scala.core.Vertx
import org.vertx.scala.core.json.JsonObject

import scala.reflect.io.Path

class TableauxConfig(
    override val vertx: Vertx,
    val authConfig: JsonObject,
    databaseConfig: JsonObject,
    workingDirectory: String,
    uploadsDirectory: String,
    val rolePermissions: JsonObject,
    val openApiUrl: Option[String] = None
) extends VertxAccess {

  def uploadsDirectoryPath(): Path = {
    retrievePath(uploadsDirectory)
  }

  def retrievePath(subpath: String): Path = {
    Path(s"$workingDirectory/$subpath")
  }

  def isWorkingDirectoryAbsolute: Boolean = {
    workingDirectory.startsWith("/")
  }
}
