package com.campudus.tableaux

import io.vertx.scala.ScalaVerticle
import org.vertx.scala.core.json._

import scala.io.Source
import scala.reflect.io.Path

trait TestConfig {

  val verticle: ScalaVerticle

  lazy val config = jsonFromFile("conf-test.json", "conf-travis.json", "../conf-test.json", "../conf-travis.json")

  lazy val port = config.getInteger("port", Starter.DEFAULT_PORT)
  lazy val databaseConfig = config.getJsonObject("database", Json.obj())
  lazy val workingDirectory = config.getString("workingDirectory")
  lazy val uploadsDirectory = config.getString("uploadsDirectory")
  lazy val uploadsTempDirectory = config.getString("uploadsTempDirectory")

  lazy val tableauxConfig = TableauxConfig(verticle, databaseConfig, workingDirectory, uploadsDirectory, uploadsTempDirectory)

  private def readTextFile(filePath: String): String = Source.fromFile(filePath).getLines().mkString

  private def jsonFromFile(filePaths: String*): JsonObject = {
    filePaths.find(Path(_).exists).map(path => Json.fromObjectString(readTextFile(path))).get
  }
}
