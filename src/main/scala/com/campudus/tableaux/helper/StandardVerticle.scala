package com.campudus.tableaux.helper

import com.typesafe.scalalogging.LazyLogging
import io.vertx.scala.ScalaVerticle

trait StandardVerticle extends LazyLogging {
  val verticle: ScalaVerticle

  lazy val vertx = verticle.getVertx

  implicit lazy val executionContext = verticle.executionContext
}
