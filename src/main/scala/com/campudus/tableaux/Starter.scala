package com.campudus.tableaux

import org.vertx.scala.core.FunctionConverters._
import org.vertx.scala.core.json.Json
import org.vertx.scala.platform.Verticle

import scala.concurrent.{Future, Promise}
import scala.util.{Try, Failure, Success}

class Starter extends Verticle {

  override def start(p: Promise[Unit]): Unit = {
    val modDeploy = Promise[Unit]()
    println("hello")
    container.deployModule("io.vertx~mod-mysql-postgresql_2.11~0.3.1", Json.obj(), 1, {
      case Success(id) => modDeploy.success()
      case Failure(ex) => modDeploy.failure(ex)
    }: Try[String] => Unit)

    p.completeWith(Future.sequence(List(modDeploy.future, deployHttpServer)).map(_ => ()))
  }

  def deployHttpServer: Future[Unit] = {
    println("httpServer")
    vertx.createHttpServer.requestHandler(new TableauxRouter(container, logger, vertx)).listen(8181)
    Future.successful()
  }
}
