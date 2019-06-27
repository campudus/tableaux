package com.campudus.tableaux.router.auth.permission

sealed trait Action {
  val name: String
  override def toString: String = name
}

object Action {

  def apply(action: String): Action = {
    action match {
      case Create.name => Create
      case View.name => View
      case Edit.name => Edit
      case Delete.name => Delete
      case EditDisplayProperty.name => EditDisplayProperty
      case CreateRow.name => CreateRow
      case DeleteRow.name => DeleteRow
      case EditStructureProperty.name => EditStructureProperty
      case EditCellAnnotation.name => EditCellAnnotation
      case EditRowAnnotation.name => EditRowAnnotation
      case ViewCellValue.name => ViewCellValue
      case EditCellValue.name => EditCellValue
      case _ => throw new IllegalArgumentException(s"Invalid argument for PermissionType $action")
    }
  }
}

case object Create extends Action { override val name = "create" }
case object View extends Action { override val name = "view" }
case object Edit extends Action { override val name = "edit" } // only for media
case object Delete extends Action { override val name = "delete" }
case object EditDisplayProperty extends Action { override val name = "editDisplayProperty" }
case object CreateRow extends Action { override val name = "createRow" }
case object DeleteRow extends Action { override val name = "deleteRow" }
case object EditStructureProperty extends Action { override val name = "editStructureProperty" }
case object EditCellAnnotation extends Action { override val name = "editCellAnnotation" }
case object EditRowAnnotation extends Action { override val name = "editRowAnnotation" }
case object ViewCellValue extends Action { override val name = "viewCellValue" }
case object EditCellValue extends Action { override val name = "editCellValue" }
