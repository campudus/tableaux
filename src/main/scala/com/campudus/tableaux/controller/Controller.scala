package com.campudus.tableaux.controller

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseQuery
import com.campudus.tableaux.helper.VertxAccess
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx

import scala.concurrent.ExecutionContext

trait Controller[T <: DatabaseQuery] extends VertxAccess {

  protected val config: TableauxConfig
  protected val repository: T

  override val vertx: Vertx = config.vertx
}
