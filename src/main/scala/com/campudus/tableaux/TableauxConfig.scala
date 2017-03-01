package com.campudus.tableaux

import com.campudus.tableaux.helper.VertxAccess
import org.vertx.scala.core.json.JsonObject
import io.vertx.scala.ScalaVerticle

import scala.reflect.io.Path

object TableauxConfig {
  def apply(verticle: ScalaVerticle, databaseConfig: JsonObject, workingDir: String, uploadsDir: String): TableauxConfig = {
    val _verticle = verticle

    new TableauxConfig {
      override val verticle = _verticle
      override val database = databaseConfig
      override val workingDirectory = workingDir
      override val uploadsDirectory = uploadsDir
    }
  }
}

trait TableauxConfig extends VertxAccess {
  val database: JsonObject

  val workingDirectory: String
  val uploadsDirectory: String

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
