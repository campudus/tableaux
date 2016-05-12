package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import org.vertx.scala.core.json._

case class Link[A](id: RowId, value: A) extends DomainObject {
  override def getJson: JsonObject = Json.obj("id" -> id, "value" -> value)
}

object LinkDirection {
  /**
    * Depending on the point of view, the link may be in different directions (left-to-right or right-to-left). This
    * method retrieves the correct link direction which can be used to get SQL and the correct table ids.
    *
    * @param fromTableId The table that was used to start from.
    * @param tableId1 The table we found in the database in column 1.
    * @param tableId2 The table we found in the database in column 2.
    * @return The correct link direction with the respective values.
    */
  def apply(fromTableId: TableId, tableId1: TableId, tableId2: TableId): LinkDirection = {
    // we need this because links can go both ways
    if (fromTableId == tableId1) {
      LeftToRight(tableId1, tableId2)
    } else {
      RightToLeft(tableId2, tableId1)
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
  val from: Long

  val to: Long

  def fromSql: String

  def toSql: String

  def fromOrdering: String

  def toOrdering: String
}

case class LeftToRight(from: Long, to: Long) extends LinkDirection {
  override def fromSql: String = "id_1"

  override def toSql: String = "id_2"

  override def fromOrdering: String = "ordering_1"

  override def toOrdering: String = "ordering_2"
}

case class RightToLeft(from: Long, to: Long) extends LinkDirection {
  override def fromSql: String = "id_2"

  override def toSql: String = "id_1"

  override def fromOrdering: String = "ordering_2"

  override def toOrdering: String = "ordering_1"
}
