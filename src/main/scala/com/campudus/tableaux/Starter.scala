package com.campudus.tableaux

import com.campudus.tableaux.router.TableauxRouter
import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.http.HttpServer
import org.vertx.scala.core.json.{Json, JsonObject}
import org.vertx.scala.platform.{Container, Verticle}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object Starter {
  val DEFAULT_PORT = 8181
  val DEFAULT_DATABASE_ADDRESS = "campudus.asyncdb"
}

class Starter extends Verticle {

  override def start(p: Promise[Unit]): Unit = {
    val port = container.config().getInteger("port", Starter.DEFAULT_PORT)

    val databaseConfig = container.config().getObject("database", Json.obj())
    val validatorConfig = container.config().getObject("validator", Json.obj())

    val databaseAddress = databaseConfig.getString("address", Starter.DEFAULT_DATABASE_ADDRESS)

    for {
      _ <- deployMod(container, "io.vertx~mod-mysql-postgresql_2.11~0.3.1", databaseConfig, 1)
      _ <- deployMod(container, "com.campudus~vertx-tiny-validator4~1.0.0", validatorConfig, 1)
      _ <- deployHttpServer(port, databaseAddress)
    } yield {
      p.success()
    }
  }

  def deployMod(container: Container, modName: String, config: JsonObject, instances: Int): Future[String] = {
    val p = Promise[String]()
    container.deployModule(modName, config, instances, {
      case Success(deploymentId) => p.success(deploymentId)
      case Failure(x) => p.failure(x)
    }: Try[String] => Unit)
    p.future
  }

  def deployHttpServer(port: Int, databaseAddress: String): Future[HttpServer] = {
    val p = Promise[HttpServer]()
    val r = new TableauxRouter(this, databaseAddress)
    vertx.createHttpServer().requestHandler(r).listen(port, {
      case Success(srv) => p.success(srv)
      case Failure(ex) => p.failure(ex)
    }: Try[HttpServer] => Unit)
    p.future
  }
}
