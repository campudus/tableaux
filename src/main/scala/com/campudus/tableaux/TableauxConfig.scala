package com.campudus.tableaux

import com.campudus.tableaux.helper.VertxAccess
import io.vertx.lang.scala.{ScalaVerticle, VertxExecutionContext}
import io.vertx.scala.core.Vertx
import org.vertx.scala.core.json.JsonObject

import scala.reflect.io.Path

class TableauxConfig(
    override val vertx: Vertx,
    databaseConfig: JsonObject,
    workingDirectory: String,
    uploadsDirectory: String
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
