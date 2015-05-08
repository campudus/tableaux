package com.campudus.tableaux.helper

import com.campudus.tableaux.helper.FutureUtils._
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.buffer.Buffer
import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object FileUtils {
  def apply(verticle: Verticle): FileUtils = {
    new FileUtils(verticle)
  }
}

class FileUtils(override val verticle: Verticle) extends StandardVerticle {

  def readDir(dir: String, filter: String): Future[Array[String]] = promisify { p: Promise[Array[String]] =>
    vertx.fileSystem.readDir(dir, filter, {
      case Success(files) => p.success(files)
      case Failure(ex) =>
        logger.info("Failed reading schema directory")
        p.failure(ex)
    }: Try[Array[String]] => Unit)
  }

  def readJsonFile(fileName: String): Future[JsonObject] = promisify { p: Promise[JsonObject] =>
    vertx.fileSystem.readFile(fileName, {
      case Success(b) => p.success(Json.fromObjectString(b.toString()))
      case Failure(ex) =>
        logger.info("Failed reading schema file")
        p.failure(ex)
    }: Try[Buffer] => Unit)
  }
}