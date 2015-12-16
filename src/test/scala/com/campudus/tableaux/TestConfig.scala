package com.campudus.tableaux

import java.net.ServerSocket

import io.vertx.scala.ScalaVerticle
import org.vertx.scala.core.json._

import scala.io.Source
import scala.reflect.io.Path

trait TestConfig {

  val verticle: ScalaVerticle

  lazy val config = {
    jsonFromFile("conf-test.json", "conf-travis.json", "../conf-test.json", "../conf-travis.json")
      .put("port", port)
  }

  lazy val port = getFreePort
  lazy val databaseConfig = config.getJsonObject("database", Json.obj())
  lazy val workingDirectory = config.getString("workingDirectory")
  lazy val uploadsDirectory = config.getString("uploadsDirectory")

  lazy val tableauxConfig = TableauxConfig(verticle, databaseConfig, workingDirectory, uploadsDirectory)

  private def readTextFile(filePath: String): String = Source.fromFile(filePath).getLines().mkString

  private def jsonFromFile(filePaths: String*): JsonObject = {
    filePaths.find(Path(_).exists).map(path => Json.fromObjectString(readTextFile(path))).get
  }

  private def autoClose[A, B <: AutoCloseable](resource: B, block: (B) => A): A = {
    var t: Throwable = null
    try {
      block(resource)
    } catch {
      case x: Throwable => t = x; throw x
    } finally {
      if (resource != null) {
        if (t != null) {
          try {
            resource.close()
          } catch {
            case y: Throwable => t.addSuppressed(y)
          }
        } else {
          resource.close()
        }
      }
    }
  }

  private def getFreePort: Int = {
    autoClose(new ServerSocket(0), {
      socket: ServerSocket =>
        socket.getLocalPort
    })
  }
}
