package com.campudus.tableaux.helper

import io.vertx.core.Vertx
import io.vertx.lang.scala.VertxExecutionContext

import com.typesafe.scalalogging.LazyLogging

trait VertxAccess extends LazyLogging {

  val vertx: Vertx

  implicit lazy val executionContext: VertxExecutionContext =
    VertxExecutionContext(vertx, vertx.getOrCreateContext())
}
