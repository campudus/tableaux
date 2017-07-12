package com.campudus.tableaux.testtools

import java.net.ServerSocket

import com.campudus.tableaux.TableauxConfig
import org.vertx.scala.core.json._

import scala.io.Source
import scala.reflect.io.Path

trait TestConfig {

  lazy val fileConfig: JsonObject = jsonFromFile(
    "conf-test.json",
    "conf-travis.json",
    "../conf-test.json",
    "../conf-travis.json"
  )

  var host: String

  var port: Int

  var databaseConfig: JsonObject

  var tableauxConfig: TableauxConfig

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

  protected def getFreePort: Int = {
    autoClose(new ServerSocket(0), (socket: ServerSocket) => socket.getLocalPort)
  }
}
