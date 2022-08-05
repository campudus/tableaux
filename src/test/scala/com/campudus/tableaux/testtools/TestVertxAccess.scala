package com.campudus.tableaux.testtools

import com.campudus.tableaux.helper.VertxAccess

import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.Vertx

import com.typesafe.scalalogging.LazyLogging

trait TestVertxAccess extends LazyLogging {

  var vertx: Vertx

  implicit var executionContext: VertxExecutionContext

  def vertxAccess(): VertxAccess = new VertxAccess {
    override val vertx: Vertx = TestVertxAccess.this.vertx
  }
}
