package com.campudus.tableaux

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.router.RouterRegistry
import io.vertx.core.http.{HttpServer, HttpServerRequest}
import io.vertx.ext.web.{Router, RoutingContext}
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.FutureHelper._
import io.vertx.scala.{SQLConnection, ScalaVerticle}
import org.vertx.scala.core.json.Json

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object Starter {
  val DEFAULT_PORT = 8181
  val DEFAULT_HOST = "0.0.0.0"
}

class Starter extends ScalaVerticle {

  private var connection: SQLConnection = _

  override def start(p: Promise[Unit]): Unit = {
    if (context.config().isEmpty) {
      logger.error("Provide a config please!")
      p.failure(new Exception("Provide a config please!"))
    }

    val config = context.config()

    val port = config.getInteger("port", Starter.DEFAULT_PORT)
    val host = config.getString("host", Starter.DEFAULT_HOST)

    val databaseConfig = config.getJsonObject("database", Json.obj())

    val tableauxConfig = TableauxConfig(
      verticle = this,
      databaseConfig = databaseConfig,
      workingDir = config.getString("workingDirectory"),
      uploadsDir = config.getString("uploadsDirectory")
    )

    connection = SQLConnection(this, databaseConfig)

    (for {
      _ <- createUploadsDirectories(tableauxConfig)
      _ <- deployHttpServer(port, host, tableauxConfig, connection)
    } yield {
        p.success(())
      }).recover({
      case t: Throwable => p.failure(t)
    })
  }

  override def stop(p: Promise[Unit]): Unit = {
    connection.close()
    p.success(())
  }

  def createUploadsDirectories(config: TableauxConfig): Future[Unit] = {
    FileUtils(this).mkdirs(config.uploadsDirectoryPath())
  }

  def handleUpload(context: RoutingContext): Unit = {
    logger.info(s"handle upload ${context.fileUploads()}")

    context.response().end()
  }

  def deployHttpServer(port: Int, host: String, tableauxConfig: TableauxConfig, connection: SQLConnection): Future[HttpServer] = futurify { p: Promise[HttpServer] =>
    val dbConnection = DatabaseConnection(this, connection)
    val routerRegistry = RouterRegistry(tableauxConfig, dbConnection)

    val router = Router.router(vertx)

    router.route().handler(routerRegistry)

    vertx.createHttpServer().requestHandler(router.accept(_: HttpServerRequest)).listen(port, host, {
      case Success(server) => p.success(server)
      case Failure(x) => p.failure(x)
    }: Try[HttpServer] => Unit)
  }
}
