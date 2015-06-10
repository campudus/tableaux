package com.campudus.tableaux

import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle

import scala.io.Source

trait TestConfig {

  val verticle: Verticle

  lazy val config: JsonObject = jsonFromFile("../conf-test.json")

  lazy val port: Int = config.getInteger("port", Starter.DEFAULT_PORT)
  lazy val databaseAddress: String = config.getObject("database", Json.obj()).getString("address", Starter.DEFAULT_DATABASE_ADDRESS)
  lazy val workingDirectory: String = config.getString("workingDirectory")
  lazy val uploadsDirectory: String = config.getString("uploadsDirectory")

  lazy val tableauxConfig = TableauxConfig(verticle, databaseAddress, workingDirectory, uploadsDirectory)

  private def readJsonFile(f: String): String = Source.fromFile(f).getLines().mkString
  private def jsonFromFile(f: String): JsonObject = Json.fromObjectString(readJsonFile(f))
}
