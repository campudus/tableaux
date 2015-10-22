package com.campudus.tableaux

import com.campudus.tableaux.helper.StandardVerticle
import io.vertx.core.Verticle
import io.vertx.core.json.JsonObject
import io.vertx.scala.ScalaVerticle

object TableauxConfig {
  def apply(verticle: ScalaVerticle, databaseConfig: JsonObject, workingDir: String, uploadDir: String): TableauxConfig = {
    val _verticle = verticle

    new TableauxConfig {
      override val verticle = _verticle
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
