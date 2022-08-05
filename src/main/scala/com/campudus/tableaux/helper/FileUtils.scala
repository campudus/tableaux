package com.campudus.tableaux.helper

import java.nio.file.FileAlreadyExistsException

import io.vertx.scala.core.Vertx
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future
import scala.io.Source
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}
import scala.reflect.io.File
import scala.collection.JavaConverters._

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

  private def readSchemaFromFS(indexJson: JsonArray): Future[List[JsonObject]] = {
    val asList = indexJson.asScala.toList
    var keyList: List[String] = List()
    val asListOfFutures = asList.map(obj => {
      val descriptor = obj.asInstanceOf[JsonObject]
      val key = descriptor.getString("key")
      keyList = keyList :+ key
      val file = descriptor.getString("file")
      asyncReadResourceFile(s"/JsonSchema/$file")
    })
    val asFutureList = Future.sequence(asListOfFutures)
    asFutureList.map(list => {
      val zipped = keyList zip list
      zipped.map(tuple => {
        val (key, schemaString) = tuple
        Json.obj("key" -> key, "schema" -> new JsonObject(schemaString))
      })
    })
  }

  private def asyncReadResourceFile(path: String): Future[String] = {
    Future {
      scala.io.Source.fromInputStream(getClass.getResourceAsStream(path), "UTF-8").mkString
    }
  }

  def getSchemaList(): Future[List[JsonObject]] = {

    for {
      indexString <- asyncReadResourceFile("/JsonSchema/index.json")
      schemas <- readSchemaFromFS(Json.fromArrayString(indexString))
    } yield { schemas }
  }

}
