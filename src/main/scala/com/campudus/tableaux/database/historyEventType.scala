package com.campudus.tableaux.database

sealed trait HistoryEventType {
  val eventName: String
  override def toString: String = eventName
}

object HistoryEventType {
  final val CELL_CHANGED = "cell_changed"
  final val ROW_CREATED = "row_created"

  def apply(eventType: String): HistoryEventType = {
    Option(eventType) match {
      case Some(CELL_CHANGED) => CellChangedEvent
      case Some(ROW_CREATED) => RowCreatedEvent
      case _ => throw new IllegalArgumentException(s"Invalid argument for HistoryEventType: '$eventType'")
    }
  }
}

case object CellChangedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.CELL_CHANGED
}

case object RowCreatedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.ROW_CREATED
}
