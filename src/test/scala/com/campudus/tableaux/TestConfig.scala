package com.campudus.tableaux

import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle

import scala.io.Source
import scala.reflect.io.Path

trait TestConfig {

  val verticle: Verticle

  lazy val config: JsonObject = jsonFromFile("../conf-test.json", "../conf-travis.json")

  lazy val port: Int = config.getInteger("port", Starter.DEFAULT_PORT)
  lazy val databaseAddress: String = config.getObject("database", Json.obj()).getString("address", Starter.DEFAULT_DATABASE_ADDRESS)
  lazy val workingDirectory: String = config.getString("workingDirectory")
  lazy val uploadsDirectory: String = config.getString("uploadsDirectory")

  lazy val tableauxConfig = TableauxConfig(verticle, databaseAddress, workingDirectory, uploadsDirectory)

  private def readJsonFile(f: String): String = Source.fromFile(f).getLines().mkString

  private def jsonFromFile(f1: String, f2: String): JsonObject = {
    if (Path(f1).exists) {
      Json.fromObjectString(readJsonFile(f1))
    } else {
      Json.fromObjectString(readJsonFile(f2))
    }
  }
}
