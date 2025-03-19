package com.campudus.tableaux

import com.campudus.tableaux.helper.VertxAccess

import io.vertx.scala.core.Vertx
import org.vertx.scala.core.json.JsonObject

import scala.reflect.io.Path

class TableauxConfig(
    override val vertx: Vertx,
    val authConfig: JsonObject,
    val databaseConfig: JsonObject,
    val cdnConfig: JsonObject,
    val workingDirectory: String,
    val uploadsDirectory: String,
    val rolePermissions: JsonObject,
    val openApiUrl: Option[String] = None,
    val isPublicFileServerEnabled: Boolean = false,
    var isRowPermissionCheckEnabled: Boolean = false
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
