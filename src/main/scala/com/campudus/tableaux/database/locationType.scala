package com.campudus.tableaux.database

import com.campudus.tableaux.database.model.TableauxModel.RowId

sealed trait LocationType[+T]

object LocationType {

  def apply[T](location: String, relativeTo: Option[T]): LocationType[T] = {
    (location, relativeTo) match {
      case ("start", None) => LocationStart.asInstanceOf[LocationType[T]]
      case ("end", None) => LocationEnd.asInstanceOf[LocationType[T]]
      case ("before", Some(id)) => LocationBefore(id).asInstanceOf[LocationType[T]]
      case _ => throw new IllegalArgumentException("Invalid location and/or relativeTo id.")
    }
  }
}

case object LocationStart extends LocationType[Nothing]

case object LocationEnd extends LocationType[Nothing]

case class LocationBefore[T](relativeTo: T) extends LocationType[T]
