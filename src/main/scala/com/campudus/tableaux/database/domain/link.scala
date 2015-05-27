package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel
import TableauxModel._
import org.vertx.scala.core.json._

case class Link[A](value: Seq[(IdType, A)]) {
  def toJson: Seq[JsonObject] = value map {
    case (id, v) => Json.obj("id" -> id, "value" -> v)
  }
}