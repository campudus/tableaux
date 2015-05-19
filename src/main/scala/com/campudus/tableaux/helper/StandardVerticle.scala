package com.campudus.tableaux.helper

import org.vertx.scala.core.{VertxAccess, VertxExecutionContext}
import org.vertx.scala.platform.Verticle

trait StandardVerticle extends VertxAccess {
  val verticle: Verticle

  lazy val vertx = verticle.vertx
  lazy val container = verticle.container
  lazy val logger = verticle.logger

  override implicit val executionContext = VertxExecutionContext.fromVertx(vertx, logger)
}
