package com.campudus.tableaux.helper

import java.io.FileInputStream
import java.nio.file.FileAlreadyExistsException

import io.vertx.scala.core.Vertx
import org.vertx.scala.core.json.JsonObject

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import org.vertx.scala.core.json.Json

import scala.io.Source

object FileUtils {

  def apply(vertxAccess: VertxAccess): FileUtils = {
    new FileUtils(vertxAccess)
  }
}

class FileUtils(vertxAccess: VertxAccess) extends VertxAccess {

  override val vertx: Vertx = vertxAccess.vertx

  def mkdirs(dir: Path): Future[Unit] = {
    vertx
      .fileSystem()
      .mkdirsFuture(dir.toString())
      .recoverWith({
        case _: FileAlreadyExistsException =>
          Future.successful(())
        case ex =>
          Future.failed(ex)
      })
  }

  private def withFile[A](filename: String)(func: Iterator[String] => A): A = {
    val source = Source.fromFile(filename)
    val lines = source.getLines()
    try {
      func(lines)
    } finally {
      source.close()
    }
  }

  def readJsonFile(filename: String): JsonObject = {
    val rawJsonString = withFile(filename)(_.mkString)
    Json.fromObjectString(rawJsonString)
  }

  def asyncReadJsonFile(filename: String): Future[JsonObject] = {
    for {
      ff <- asyncReadFile(filename)(_.mkString)
      json = Json.fromObjectString(ff)
    } yield json
  }

  private def asyncReadFile[T](filename: String)(func: Iterator[String] => T): Future[T] = {
    Future {
      withFile(filename)(func)
    }
  }

}
