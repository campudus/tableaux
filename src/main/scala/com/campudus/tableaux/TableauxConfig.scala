package com.campudus.tableaux

import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.platform.Verticle

object TableauxConfig {
  def apply(vert: Verticle, addr: String, pwd: String, upload: String): TableauxConfig = {
    new TableauxConfig {
      override val verticle = vert
      override val databaseAddress = addr
      override val workingDirectory = pwd
      override val uploadDirectory = upload
    }
  }
}

trait TableauxConfig extends StandardVerticle {
  val databaseAddress: String
  val workingDirectory: String
  val uploadDirectory: String
}
