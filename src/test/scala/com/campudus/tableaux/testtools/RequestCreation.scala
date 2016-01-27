package com.campudus.tableaux.testtools

import io.vertx.core.json.JsonObject
import org.vertx.scala.core.json.Json

object RequestCreation {

  sealed class ColType(name: String, kind: String) {
    def json: JsonObject = Json.obj("kind" -> kind, "name" -> name)
  }

  case class Text(name: String) extends ColType(name, "text")

  case class Numeric(name: String) extends ColType(name, "numeric")

}
