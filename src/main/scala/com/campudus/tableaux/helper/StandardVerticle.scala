package com.campudus.tableaux.helper

import org.vertx.scala.core.{VertxAccess, VertxExecutionContext}
import org.vertx.scala.platform.Verticle

trait StandardVerticle extends VertxAccess {
  val verticle: Verticle

  val vertx = verticle.vertx
  val container = verticle.container
  val logger = verticle.logger

  override implicit val executionContext = VertxExecutionContext.fromVertx(vertx, logger)
}
