package com.campudus.tableaux

import java.nio.file.FileAlreadyExistsException

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.helper.FutureUtils
import com.campudus.tableaux.router.RouterRegistry
import io.vertx.core.http.{HttpServerRequest, HttpServer}
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.{SQLConnection, ScalaVerticle}
import org.vertx.scala.core.json.Json

import scala.concurrent.{Future, Promise}
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

object Starter {
  val DEFAULT_PORT = 8181
  val DEFAULT_HOST = "0.0.0.0"
  val DEFAULT_DATABASE_ADDRESS = "campudus.asyncdb"
}

class Starter extends ScalaVerticle {

  import FutureUtils._
  import Starter._

  override def start(p: Promise[Unit]): Unit = {
    //val config = context.config()

    val config = Json.fromObjectString("""{
                            |  "port": 8181,
                            |  "host": "127.0.0.1",
                            |  "uploadsDirectory": "uploads/",
                            |  "workingDirectory": "../",
                            |  "database": {
                            |    "url": "jdbc:postgresql://localhost:5432/buschjost",
                            |    "driver_class": "org.postgresql.Driver",
                            |    "user": "alexandervetter",
                            |    "password": null
                            |  }
                            |}""".stripMargin)

    val port = config.getInteger("port", DEFAULT_PORT)
    val host = config.getString("host", DEFAULT_HOST)

    val databaseConfig = config.getJsonObject("database", Json.obj())

    val tableauxConfig = TableauxConfig(
      vert = this,
      databaseConfig = databaseConfig,
      workingDir = config.getString("workingDirectory"),
      uploadDir = config.getString("uploadsDirectory")
    )

    val connection = SQLConnection(vertx, databaseConfig)

    (for {
      _ <- createUploadsDirectory(tableauxConfig)

      _ <- deployHttpServer(port, host, tableauxConfig, connection)
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

  def deployHttpServer(port: Int, host: String, tableauxConfig: TableauxConfig, connection: SQLConnection): Future[HttpServer] = promisify { p: Promise[HttpServer] =>
    val dbConnection = DatabaseConnection(connection)
    val routerRegistry = RouterRegistry(tableauxConfig, dbConnection)

    val router = Router.router(vertx)

    router.route.handler(routerRegistry)

    vertx.createHttpServer().requestHandler(router.accept(_: HttpServerRequest)).listen(port, host, {
      case Success(server) => p.success(server)
      case Failure(x) => p.failure(x)
    }: Try[HttpServer] => Unit)
  }
}
