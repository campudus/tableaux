package com.campudus.tableaux.helper

import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.vertx.core.Verticle
import io.vertx.scala.VertxExecutionContext
import org.slf4j.LoggerFactory

trait StandardVerticle extends VertxExecutionContext with LazyLogging {
  val verticle: Verticle

  lazy val vertx = verticle.getVertx
}
