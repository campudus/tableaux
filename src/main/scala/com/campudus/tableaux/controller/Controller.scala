package com.campudus.tableaux.controller

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseQuery
import com.campudus.tableaux.helper.StandardVerticle
import io.vertx.core.Verticle
import io.vertx.scala.ScalaVerticle

trait Controller[T <: DatabaseQuery] extends StandardVerticle {

  protected val config: TableauxConfig
  protected val repository: T

  override val verticle: ScalaVerticle = config.verticle
}
