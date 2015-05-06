package com.campudus.tableaux.controller

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseQuery
import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.platform.Verticle

/**
 * Created by alexandervetter on 06.05.15.
 */
trait Controller[E <: DatabaseQuery] extends StandardVerticle {

  protected val config: TableauxConfig
  protected val repository: E

  override val verticle: Verticle = config.verticle
}
