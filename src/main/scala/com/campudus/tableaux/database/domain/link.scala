package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Link[A](id: RowId, value: A) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "value" -> value)
}

object LinkDirection {
  def apply(fromTableId: TableId, tableId1: TableId, tableId2: TableId): (LinkDirection, TableId) = {
    // we need this because links can go both ways
    if (fromTableId == tableId1) {
      (LeftToRight, tableId2)
    } else {
      (RightToLeft, tableId1)
    }
  }
}

/**
  * LinkColumn (created at Table A) points to Table B
  * Retrieve link values for Table A => LeftToRight
  * Retrieve link values for Table B => RightToLeft
  * More less it depends on the point of view.
  **/
sealed trait LinkDirection {
  def fromLinkColumn: String

  def toLinkColumn: String

  def linkTableColumns: (String, String) = (fromLinkColumn, toLinkColumn)
}

case object LeftToRight extends LinkDirection {
  override def fromLinkColumn: String = "id_1"

  override def toLinkColumn: String = "id_2"
}

case object RightToLeft extends LinkDirection {
  override def fromLinkColumn: String = "id_2"

  override def toLinkColumn: String = "id_1"
}
