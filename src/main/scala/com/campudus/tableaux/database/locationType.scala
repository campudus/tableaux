package com.campudus.tableaux.database

import com.campudus.tableaux.database.model.TableauxModel.RowId

sealed trait LocationType

object LocationType {
  def apply(location: String, relativeTo: Option[RowId]): LocationType = {
    (location, relativeTo) match {
      case ("start", None) => LocationStart
      case ("end", None) => LocationEnd
      case ("before", Some(rowId)) => LocationBefore(rowId)
      case _ => throw new IllegalArgumentException("Invalid location and/or relativeTo row id.")
    }
  }
}

case object LocationStart extends LocationType

case object LocationEnd extends LocationType

case class LocationBefore(relativeTo: RowId) extends LocationType