package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._

import org.vertx.scala.core.json._

case class Cardinality(from: Int, to: Int)

object DefaultCardinality extends Cardinality(0, 0)

case class Constraint(
    cardinality: Cardinality,
    deleteCascade: Boolean = false,
    archiveCascade: Boolean = false,
    finalCascade: Boolean = false
) extends DomainObject {

  override def getJson: JsonObject = {
    if (this == DefaultConstraint) {
      Json.emptyObj()
    } else {
      Json.obj(
        "cardinality" -> Json.obj(
          "from" -> cardinality.from,
          "to" -> cardinality.to
        ),
        "deleteCascade" -> deleteCascade,
        "archiveCascade" -> archiveCascade,
        "finalCascade" -> finalCascade
      )
    }
  }
}

object DefaultConstraint extends Constraint(DefaultCardinality, false, false)

object LinkDirection {

  /**
    * Depending on the point of view, the link may be in different directions (left-to-right or right-to-left). This
    * method retrieves the correct link direction which can be used to get SQL and the correct table ids.
    *
    * @param fromTableId
    *   The table that was used to start from.
    * @param tableId1
    *   The table we found in the database in column 1.
    * @param tableId2
    *   The table we found in the database in column 2.
    * @return
    *   The correct link direction with the respective values.
    */
  def apply(
      fromTableId: TableId,
      tableId1: TableId,
      tableId2: TableId,
      cardinality1: Int,
      cardinality2: Int,
      deleteCascade: Boolean,
      archiveCascade: Boolean,
      finalCascade: Boolean
  ): LinkDirection = {

    // we need this because links can go both ways
    if (fromTableId == tableId1) {
      LeftToRight(
        tableId1,
        tableId2,
        Constraint(Cardinality(cardinality1, cardinality2), deleteCascade, archiveCascade, finalCascade)
      )
    } else {
      // no cascade functions in this direction
      RightToLeft(
        tableId2,
        tableId1,
        Constraint(
          Cardinality(cardinality2, cardinality1),
          deleteCascade = false,
          archiveCascade = false,
          finalCascade = false
        )
      )
    }
  }
}

/**
  * LinkColumn (created at Table A) points to Table B Retrieve link values for Table A => LeftToRight Retrieve link
  * values for Table B => RightToLeft More less it depends on the point of view.
  */
sealed trait LinkDirection {
  val from: TableId

  val to: TableId

  val constraint: Constraint

  def fromSql: String

  def toSql: String

  def orderingSql: String

  def fromCardinality: String

  def toCardinality: String

  def isManyToMany: Boolean = constraint.cardinality.from == 0 && constraint.cardinality.to == 0
}

case class LeftToRight(from: TableId, to: TableId, constraint: Constraint) extends LinkDirection {

  override def fromSql: String = "id_1"

  override def toSql: String = "id_2"

  override def orderingSql: String = "ordering_1"

  override def fromCardinality: String = {
    if (constraint.cardinality.from == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_1"
    }
  }

  override def toCardinality: String = {
    if (constraint.cardinality.to == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_2"
    }
  }
}

case class RightToLeft(from: TableId, to: TableId, constraint: Constraint) extends LinkDirection {

  override def fromSql: String = "id_2"

  override def toSql: String = "id_1"

  override def orderingSql: String = "ordering_2"

  override def fromCardinality: String = {
    if (constraint.cardinality.from == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_2"
    }
  }

  override def toCardinality: String = {
    if (constraint.cardinality.to == 0) {
      Int.MaxValue.toString
    } else {
      "cardinality_1"
    }
  }
}
