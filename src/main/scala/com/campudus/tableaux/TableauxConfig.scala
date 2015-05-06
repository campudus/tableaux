package com.campudus.tableaux

import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.platform.Verticle

/**
 * Created by alexandervetter on 06.05.15.
 */
object TableauxConfig {
  def apply(vert: Verticle, addr: String): TableauxConfig = {
    new TableauxConfig {
      override val verticle: Verticle = vert
      override val databaseAddress: String = addr
    }
  }
}

trait TableauxConfig extends StandardVerticle {
  override val verticle: Verticle

  val databaseAddress: String
}
