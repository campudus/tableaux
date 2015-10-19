package com.campudus.tableaux

import com.campudus.tableaux.helper.StandardVerticle
import io.vertx.core.Verticle
import io.vertx.core.json.JsonObject

object TableauxConfig {
  def apply(vert: Verticle, databaseConfig: JsonObject, workingDir: String, uploadDir: String): TableauxConfig = {
    new TableauxConfig {
      override val verticle = vert
      override val database = databaseConfig
      override val workingDirectory = workingDir
      override val uploadsDirectory = uploadDir
    }
  }
}

trait TableauxConfig extends StandardVerticle {
  val database: JsonObject

  val workingDirectory: String
  val uploadsDirectory: String
}
