package com.campudus.tableaux

import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.http.HttpServer
import org.vertx.scala.core.json.Json
import org.vertx.scala.platform.Verticle

import scala.concurrent.{Future, Promise}
import scala.util.{Try, Failure, Success}

class Starter extends Verticle {

  val DEFAULT_PORT = 8181
  val config = Json.obj("username" -> "postgres", "password" -> "admin")

  override def start(p: Promise[Unit]): Unit = {
    val modDeploy = Promise[Unit]()
    container.deployModule("io.vertx~mod-mysql-postgresql_2.11~0.3.1", config, 1, {
      case Success(id) => modDeploy.success()
      case Failure(ex) => modDeploy.failure(ex)
    }: Try[String] => Unit)

    val serverFuture = deployHttpServer(container.config().getInteger("port", DEFAULT_PORT))
    p.completeWith(Future.sequence(List(modDeploy.future, serverFuture)).map(_ => ()))
  }

  def deployHttpServer(port: Int): Future[HttpServer] = {
    val p = Promise[HttpServer]()
    vertx.createHttpServer().requestHandler(new TableauxRouter(this)).listen(port, {
      case Success(srv) => p.success(srv)
      case Failure(ex) => p.failure(ex)
    }: Try[HttpServer] => Unit)
    p.future
  }
}
