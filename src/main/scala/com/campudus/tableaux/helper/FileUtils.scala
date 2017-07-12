package com.campudus.tableaux.helper

import java.nio.file.FileAlreadyExistsException

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.Path

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
}
