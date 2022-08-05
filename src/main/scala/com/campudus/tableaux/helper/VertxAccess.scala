package com.campudus.tableaux.helper

import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.Vertx

import com.typesafe.scalalogging.LazyLogging

trait VertxAccess extends LazyLogging {

  val vertx: Vertx

  private lazy val jvertx: io.vertx.core.Vertx = vertx.asJava.asInstanceOf[io.vertx.core.Vertx]

  implicit lazy val executionContext: VertxExecutionContext =
    VertxExecutionContext(
      io.vertx.scala.core.Context(jvertx.getOrCreateContext())
    )
}
