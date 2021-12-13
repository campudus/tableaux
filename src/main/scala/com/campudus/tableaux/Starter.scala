package com.campudus.tableaux

import com.campudus.tableaux.cache.CacheVerticle
import com.campudus.tableaux.verticles.JsonSchemaValidator.{JsonSchemaValidatorVerticle, JsonSchemaValidatorClient}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.helper.{FileUtils, VertxAccess}
import com.campudus.tableaux.router._
import com.typesafe.scalalogging.LazyLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.http.HttpServer
import io.vertx.scala.core.{DeploymentOptions, Vertx}
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object Starter {
  val DEFAULT_HOST = "127.0.0.1"
  val DEFAULT_PORT = 8181

  val DEFAULT_WORKING_DIRECTORY = "./"
  val DEFAULT_UPLOADS_DIRECTORY = "uploads/"
  val DEFAULT_ROLE_PERMISSIONS_PATH = "./role-permissions.json"
}

class Starter extends ScalaVerticle with LazyLogging {

  private var connection: SQLConnection = _
  private var server: HttpServer = _

  override def startFuture(): Future[Unit] = {
    if (config.isEmpty) {
      logger.error("Provide a config please!")
      Future.failed(new Exception("Provide a config please!"))
    } else if (config.getJsonObject("database", Json.obj()).isEmpty) {
      logger.error("Provide a database config please!")
      Future.failed(new Exception("Provide a database config please!"))
    } else {
      val databaseConfig = config.getJsonObject("database", Json.obj())

      val cacheConfig = config.getJsonObject("cache", Json.obj())
      if (cacheConfig.isEmpty) {
        logger.warn("Cache config is empty, using default settings.")
      }
      val jsonSchemaConfig = Json.obj()

      val host = getStringDefault(config, "host", Starter.DEFAULT_HOST)
      val port = getIntDefault(config, "port", Starter.DEFAULT_PORT)
      val workingDirectory = getStringDefault(config, "workingDirectory", Starter.DEFAULT_WORKING_DIRECTORY)
      val uploadsDirectory = getStringDefault(config, "uploadsDirectory", Starter.DEFAULT_UPLOADS_DIRECTORY)
      val authConfig = config.getJsonObject("auth", Json.obj())
      val rolePermissionsPath = getStringDefault(config, "rolePermissionsPath", Starter.DEFAULT_ROLE_PERMISSIONS_PATH)

      val rolePermissions = FileUtils(vertxAccessContainer()).readJsonFile(rolePermissionsPath, Json.emptyObj())

      val tableauxConfig = new TableauxConfig(
        vertx = this.vertx,
        databaseConfig = databaseConfig,
        authConfig = authConfig,
        workingDirectory = workingDirectory,
        uploadsDirectory = uploadsDirectory,
        rolePermissions = rolePermissions
      )

      connection = SQLConnection(vertxAccessContainer(), databaseConfig)

      for {
        _ <- createUploadsDirectories(tableauxConfig)
        server <- deployHttpServer(port, host, tableauxConfig, connection)
        _ <- deployJsonSchemaValidatorVerticle(jsonSchemaConfig)
        _ <- deployCacheVerticle(cacheConfig)
      } yield {
        this.server = server
      }
    }
  }

  override def stopFuture(): Future[Unit] = {
    for {
      _ <- connection.close()
      _ <- server.closeFuture()
    } yield ()
  }

  private def createUploadsDirectories(config: TableauxConfig): Future[Unit] = {
    FileUtils(vertxAccessContainer()).mkdirs(config.uploadsDirectoryPath())
  }

  private def deployHttpServer(
      port: Int,
      host: String,
      tableauxConfig: TableauxConfig,
      connection: SQLConnection
  ): Future[HttpServer] = {
    val dbConnection = DatabaseConnection(vertxAccessContainer(), connection)

    val mainRouter = RouterRegistry.init(tableauxConfig, dbConnection)

    vertx
      .createHttpServer()
      .requestHandler(mainRouter.accept)
      .listenFuture(port, host)
  }

  private def deployJsonSchemaValidatorVerticle(config: JsonObject): Future[String] = {

    val options = DeploymentOptions()
      .setConfig(config)

      val jsonSchemaValidatorClient = JsonSchemaValidatorClient(vertx)
      val deployFuture = for {
        deployedVerticle <- vertx.deployVerticleFuture(ScalaVerticle.nameForVerticle[JsonSchemaValidatorVerticle], options)
        schemas <- FileUtils(vertxAccessContainer()).getSchemaList()
        _ <- jsonSchemaValidatorClient.registerMultipleSchemas(schemas)
      } yield (deployedVerticle)

    deployFuture.onComplete({
      case Success(id) =>
        logger.info(s"JsonSchemaValidatorVerticle deployed with ID $id")
      case Failure(e) =>
        logger.error("JsonSchemaValidatorVerticle couldn't be deployed.", e)
    })

    deployFuture
  }

  private def deployCacheVerticle(config: JsonObject): Future[String] = {
    val options = DeploymentOptions()
      .setConfig(config)

    val deployFuture = vertx.deployVerticleFuture(ScalaVerticle.nameForVerticle[CacheVerticle], options)

    deployFuture.onComplete({
      case Success(id) =>
        logger.info(s"CacheVerticle deployed with ID $id")
      case Failure(e) =>
        logger.error("CacheVerticle couldn't be deployed.", e)
    })

    deployFuture
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

  private def vertxAccessContainer(): VertxAccess = new VertxAccess {
    override val vertx: Vertx = Starter.this.vertx
  }
}
