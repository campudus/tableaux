package com.campudus.tableaux.testtools

import io.vertx.core.json.JsonObject
import org.vertx.scala.core.json.Json

object RequestCreation {

  sealed abstract class ColType(val kind: String) {
    val name: String
    def json: JsonObject = Json.obj("kind" -> kind, "name" -> name)
  }

  case class Text(name: String) extends ColType("text")

  case class Numeric(name: String) extends ColType("numeric")

  case class Multilanguage(column: ColType) extends ColType(column.kind) {
    val name: String = column.name
    override def json: JsonObject = column.json.mergeIn(Json.obj("multilanguage" -> true))
  }

  case class Identifier(column: ColType) extends ColType(column.kind) {
    val name: String = column.name
    override def json: JsonObject = column.json.mergeIn(Json.obj("identifier" -> true))
  }
}
