package com.campudus.tableaux

import java.nio.file.FileAlreadyExistsException

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.helper.FutureUtils
import com.campudus.tableaux.router.RouterRegistry
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.http.HttpServer
import org.vertx.scala.core.json.{Json, JsonObject}
import org.vertx.scala.platform.{Container, Verticle}

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

object Starter {
  val DEFAULT_PORT = 8181
  val DEFAULT_DATABASE_ADDRESS = "campudus.asyncdb"
}

class Starter extends Verticle {

  import FutureUtils._
  import Starter._

  override def start(p: Promise[Unit]): Unit = {
    val config = container.config()

    val port = config.getInteger("port", DEFAULT_PORT)

    val databaseConfig = config.getObject("database", Json.obj())
    val validatorConfig = config.getObject("validator", Json.obj())

    val databaseAddress = databaseConfig.getString("address", DEFAULT_DATABASE_ADDRESS)

    val tableauxConfig = TableauxConfig(
      vert = this,
      addr = databaseAddress,
      pwd = config.getString("workingDirectory"),
      upload = config.getString("uploadsDirectory")
    )

    (for {
      _ <- createUploadsDirectory(tableauxConfig)

      _ <- deployMod(container, "io.vertx~mod-mysql-postgresql_2.11~0.3.1", databaseConfig, 1)
      _ <- deployMod(container, "com.campudus~vertx-tiny-validator4~1.0.0", validatorConfig, 1)

      _ <- deployHttpServer(port, tableauxConfig)
    } yield ())
      .map(_ => p.success(()))
      .recover({
      case t: Throwable => p.failure(t)
    })
  }

  def createUploadsDirectory(config: TableauxConfig): Future[Unit] = promisify { p: Promise[Unit] =>
    val uploadsDirectory = Path(s"${config.workingDirectory}/${config.uploadsDirectory}")

    // succeed also in error cause (directory already exists)
    vertx.fileSystem.mkdir(s"$uploadsDirectory", {
      case Success(s) => p.success(())
      case Failure(x) => {
        x.getCause match {
          case _: FileAlreadyExistsException => p.success(())
          case _ => p.failure(x)
        }
      }
    }: Try[Void] => Unit)
  }

  def deployMod(container: Container, modName: String, config: JsonObject, instances: Int): Future[String] = promisify { p: Promise[String] =>
    container.deployModule(modName, config, instances, {
      case Success(deploymentId) => p.success(deploymentId)
      case Failure(x) => p.failure(x)
    }: Try[String] => Unit)
  }

  def deployHttpServer(port: Int, tableauxConfig: TableauxConfig): Future[HttpServer] = promisify { p: Promise[HttpServer] =>
    val dbConnection = DatabaseConnection(tableauxConfig)
    val router = RouterRegistry(tableauxConfig, dbConnection)

    vertx.createHttpServer().requestHandler(router).listen(port, {
      case Success(server) => p.success(server)
      case Failure(x) => p.failure(x)
    }: Try[HttpServer] => Unit)
  }
}
