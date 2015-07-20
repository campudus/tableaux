package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class LinkConnection(toTableId: TableId, toColumnId: ColumnId, fromColumnId: ColumnId)

case class Link[A](id: RowId, value: A) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "value" -> value)
}
