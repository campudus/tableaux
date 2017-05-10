package com.campudus.tableaux.helper

import java.nio.file.FileAlreadyExistsException

import io.vertx.scala.FunctionConverters._
import io.vertx.scala.FutureHelper._
import io.vertx.scala.ScalaVerticle

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

object FileUtils {

  def apply(verticle: ScalaVerticle): FileUtils = {
    new FileUtils(verticle)
  }
}

class FileUtils(override val verticle: ScalaVerticle) extends VertxAccess {

  def mkdirs(dir: Path): Future[Unit] = {
    futurify { p: Promise[Unit] => // succeed also in error cause (directory already exists)
    {
      vertx.fileSystem.mkdirs(
        dir.toString(), {
          case Success(s) => p.success(())
          case Failure(x) => {
            x.getCause match {
              case _: FileAlreadyExistsException => p.success(())
              case _ => p.failure(x)
            }
          }
        }: Try[Void] => Unit
      )
    }
    }
  }
}
