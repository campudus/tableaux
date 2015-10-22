package com.campudus.tableaux.helper

import com.campudus.tableaux.helper.FutureUtils._
import io.vertx.core.Verticle
import io.vertx.core.buffer.Buffer
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.ScalaVerticle
import org.vertx.scala.core.json._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object FileUtils {
  def apply(verticle: ScalaVerticle): FileUtils = {
    new FileUtils(verticle)
  }
}

class FileUtils(override val verticle: ScalaVerticle) extends StandardVerticle {

  def readDir(dir: String, filter: String): Future[Array[String]] = promisify { p: Promise[Array[String]] =>
    vertx.fileSystem.readDir(dir, filter, {
      case Success(files) => {
        import scala.collection.JavaConverters._
        p.success(files.asScala.toArray)
      }
      case Failure(ex) =>
        logger.info("Failed reading schema directory")
        p.failure(ex)
    }: Try[java.util.List[String]] => Unit)
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