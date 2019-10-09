package com.campudus.tableaux.helper

import java.nio.file.FileAlreadyExistsException

import io.vertx.scala.core.Vertx
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.io.Source
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

object FileUtils {

  def apply(vertxAccess: VertxAccess): FileUtils = {
    new FileUtils(vertxAccess)
  }
}

class FileUtils(vertxAccess: VertxAccess) extends VertxAccess {

  val commentsRegex: String = "(?:/\\*(?:[^*]|(?:\\*+[^*/]))*\\*+/)|(?://.*)"

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
    val lines = source.getLines
    try {
      func(lines)
    } finally {
      source.close()
    }
  }

  def readJsonFile(filename: String, default: JsonObject): JsonObject = {
    Try {
      val rawJsonString = withFile(filename)(_.mkString("\n").replaceAll(commentsRegex, ""))
      Json.fromObjectString(rawJsonString)
    } match {
      case Success(jsonContent) => jsonContent
      case Failure(ex) => {
        logger.warn(s"Error while reading json file: ${ex.getMessage}")
        default
      }
    }

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
