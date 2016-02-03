package com.campudus.tableaux

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.helper.FileUtils
import com.campudus.tableaux.router.RouterRegistry
import io.vertx.core.http.{HttpServer, HttpServerRequest}
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.ext.web.Router
import io.vertx.scala.FunctionConverters._
import io.vertx.scala.FutureHelper._
import io.vertx.scala.{SQLConnection, ScalaVerticle}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object Starter {
  val DEFAULT_HOST = "127.0.0.1"
  val DEFAULT_PORT = 8181

  val DEFAULT_WORKING_DIRECTORY = "./"
  val DEFAULT_UPLOADS_DIRECTORY = "uploads/"
}

class Starter extends ScalaVerticle {

  private var connection: SQLConnection = _
  private var server: HttpServer = _

  override def start(p: Promise[Unit]): Unit = {
    if (context.config().isEmpty) {
      logger.error("Provide a config please!")
      p.failure(new Exception("Provide a config please!"))
    } else {
      val config = context.config()

      val databaseConfig = config.getJsonObject("database", Json.obj())
      if (databaseConfig.isEmpty) {
        logger.error("Provide a database config please!")
        p.failure(new Exception("Provide a database config please!"))
      }

      val host = getStringDefault(config, "host", Starter.DEFAULT_HOST)
      val port = getIntDefault(config, "port", Starter.DEFAULT_PORT)
      val workingDirectory = getStringDefault(config, "workingDirectory", Starter.DEFAULT_WORKING_DIRECTORY)
      val uploadsDirectory = getStringDefault(config, "uploadsDirectory", Starter.DEFAULT_UPLOADS_DIRECTORY)

      val tableauxConfig = TableauxConfig(
        verticle = this,
        databaseConfig = databaseConfig,
        workingDir = workingDirectory,
        uploadsDir = uploadsDirectory
      )

      connection = SQLConnection(this, databaseConfig)

      val initialize = for {
        _ <- createUploadsDirectories(tableauxConfig)
        server <- deployHttpServer(port, host, tableauxConfig, connection)
      } yield {
        this.server = server
        p.success(())
      }

      initialize.recover({
        case t: Throwable => p.failure(t)
      })
    }
  }

  override def stop(p: Promise[Unit]): Unit = {
    import io.vertx.scala.FunctionConverters._

    p.completeWith({
      for {
        _ <- connection.close()
        _ <- server.close(_: Handler[AsyncResult[Void]])
      } yield ()
    })
  }

  private def createUploadsDirectories(config: TableauxConfig): Future[Unit] = {
    FileUtils(this).mkdirs(config.uploadsDirectoryPath())
  }

  private def deployHttpServer(port: Int, host: String, tableauxConfig: TableauxConfig, connection: SQLConnection): Future[HttpServer] = futurify { p: Promise[HttpServer] =>
    val dbConnection = DatabaseConnection(this, connection)
    val routerRegistry = RouterRegistry(tableauxConfig, dbConnection)

    val router = Router.router(vertx)

    router.route().handler(routerRegistry)

    vertx.createHttpServer().requestHandler(router.accept(_: HttpServerRequest)).listen(port, host, {
      case Success(server) => p.success(server)
      case Failure(x) => p.failure(x)
    }: Try[HttpServer] => Unit)
  }

  private def getStringDefault(config: JsonObject, field: String, default: String): String = {
    if (config.containsKey(field)) {
      config.getString(field)
    } else {
      logger.warn(s"No $field (config) was set. Use default '$default'.")
      default
    }
  }

  private def getIntDefault(config: JsonObject, field: String, default: Int): Int = {
    if (config.containsKey(field)) {
      config.getInteger(field).toInt
    } else {
      logger.warn(s"No $field (config) was set. Use default '$default'.")
      default
    }
  }
}
