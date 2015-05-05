package com.campudus.tableaux.database.structure

import com.campudus.tableaux.database.Tableaux._
import org.vertx.scala.core.json._

case class Link[A](value: Seq[(IdType, A)]) {
  def toJson: Seq[JsonObject] = value map {
    case (id, v) => Json.obj("id" -> id, "value" -> v)
  }
}