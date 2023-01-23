package com.campudus.tableaux.database

sealed trait HistoryType {
  def typeName: String
  override def toString: String = typeName
}

object HistoryType {
  final val Row = "row"
  final val Cell = "cell"
  final val CellComment = "cell_comment"
  final val CellFlag = "cell_flag"
  final val RowFlag = "row_flag"
  final val RowPermissions = "row_permissions"

  def apply(historyType: String): HistoryType = {
    historyType match {
      case Row => HistoryTypeRow
      case Cell => HistoryTypeCell
      case CellComment => HistoryTypeCellComment
      case CellFlag => HistoryTypeCellFlag
      case RowFlag => HistoryTypeRowFlag
      case RowPermissions => HistoryTypeRowPermissions
      case _ => throw new IllegalArgumentException(s"Invalid argument for HistoryType $historyType")
    }
  }
}

case object HistoryTypeRow extends HistoryType {
  override val typeName = HistoryType.Row
}

case object HistoryTypeCell extends HistoryType {
  override val typeName = HistoryType.Cell
}

case object HistoryTypeCellComment extends HistoryType {
  override val typeName = HistoryType.CellComment
}

case object HistoryTypeCellFlag extends HistoryType {
  override val typeName = HistoryType.CellFlag
}

case object HistoryTypeRowFlag extends HistoryType {
  override val typeName = HistoryType.RowFlag
}

case object HistoryTypeRowPermissions extends HistoryType {
  override val typeName = HistoryType.RowPermissions
}

sealed trait HistoryEventType {
  val eventName: String
  override def toString: String = eventName
}

object HistoryEventType {
  final val CellChanged = "cell_changed"
  final val RowCreated = "row_created"
  final val RowDeleted = "row_deleted"
  final val AnnotationAdded = "annotation_added"
  final val AnnotationRemoved = "annotation_removed"
  final val RowPermissionsChanged = "annotation_changed"

  def apply(eventType: String): HistoryEventType = {
    eventType match {
      case CellChanged => CellChangedEvent
      case RowCreated => RowCreatedEvent
      case RowDeleted => RowDeletedEvent
      case AnnotationAdded => AnnotationAddedEvent
      case AnnotationRemoved => AnnotationRemovedEvent
      case RowPermissionsChanged => RowPermissionsChangedEvent
      case _ => throw new IllegalArgumentException(s"Invalid argument for HistoryEventType $eventType")
    }
  }
}

case object CellChangedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.CellChanged
}

case object RowCreatedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.RowCreated
}

case object RowDeletedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.RowDeleted
}

case object AnnotationAddedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.AnnotationAdded
}

case object AnnotationRemovedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.AnnotationRemoved
}

case object RowPermissionsChangedEvent extends HistoryEventType {
  override val eventName = HistoryEventType.RowPermissionsChanged
}
