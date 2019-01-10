package com.campudus.tableaux.database

sealed trait HistoryType {
  val typeName: String
  override def toString: String = typeName
}

object HistoryType {
  final val ROW = "row"
  final val CELL = "cell"
  final val COMMENT = "comment"
  final val CELL_FLAG = "cell_flag"
  final val ROW_FLAG = "row_flag"
}

sealed trait HistoryEventType {
  val eventName: String
  override def toString: String = eventName
}

object HistoryEventType {
  final val CELL_CHANGED = "cell_changed"
  final val ROW_CREATED = "row_created"
  final val ANNOTATION_ADDED = "annotation_added"
  final val ANNOTATION_REMOVED = "annotation_removed"

  def apply(eventType: String): HistoryEventType = {
    eventType match {
      case CELL_CHANGED => CellChangedEvent
      case ROW_CREATED => RowCreatedEvent
      case ANNOTATION_ADDED => AnnotationAddedEvent
      case ANNOTATION_REMOVED => AnnotationRemovedEvent
      case _ => throw new IllegalArgumentException(s"Invalid argument for HistoryEventType $eventType")
    }
  }
}

case object CellChangedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.CELL_CHANGED
}

case object RowCreatedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.ROW_CREATED
}

case object AnnotationAddedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.ANNOTATION_ADDED
}

case object AnnotationRemovedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.ANNOTATION_REMOVED
}
