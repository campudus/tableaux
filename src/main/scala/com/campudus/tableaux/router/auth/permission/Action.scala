package com.campudus.tableaux.router.auth.permission

sealed trait Action {
  val name: String
  override def toString: String = name
}

object Action {

  def apply(action: String): Action = {
    action match {
      case ViewTable.name => ViewTable
      case EditCellAnnotation.name => EditCellAnnotation
      case CreateRow.name => CreateRow
      case DeleteRow.name => DeleteRow
      case EditRowAnnotation.name => EditRowAnnotation
      case EditTableDisplayProperty.name => EditTableDisplayProperty
      case CreateTable.name => CreateTable
      case DeleteTable.name => DeleteTable
      case EditTableStructureProperty.name => EditTableStructureProperty
      case ViewHiddenTable.name => ViewHiddenTable
      case ViewColumn.name => ViewColumn
      case EditColumnDisplayProperty.name => EditColumnDisplayProperty
      case EditColumnStructureProperty.name => EditColumnStructureProperty
      case CreateColumn.name => CreateColumn
      case DeleteColumn.name => DeleteColumn
      case ViewCellValue.name => ViewCellValue
      case EditCellValue.name => EditCellValue
      case CreateMedia.name => CreateMedia
      case EditMedia.name => EditMedia
      case DeleteMedia.name => DeleteMedia
      case CreateTableGroup.name => CreateTableGroup
      case EditTableGroup.name => EditTableGroup
      case DeleteTableGroup.name => DeleteTableGroup
      case CreateService.name => CreateService
      case DeleteService.name => DeleteService
      case ViewService.name => ViewService
      case EditServiceDisplayProperty.name => EditServiceDisplayProperty
      case EditServiceStructureProperty.name => EditServiceStructureProperty
      case EditSystem.name => EditSystem
      case _ => throw new IllegalArgumentException(s"Invalid argument for PermissionType $action")
    }
  }
}

case object ViewTable extends Action { override val name = "viewTable" }
case object EditCellAnnotation extends Action { override val name = "editCellAnnotation" }
case object CreateRow extends Action { override val name = "createRow" }
case object DeleteRow extends Action { override val name = "deleteRow" }
case object EditRowAnnotation extends Action { override val name = "editRowAnnotation" }
case object EditTableDisplayProperty extends Action { override val name = "editTableDisplayProperty" }
case object CreateTable extends Action { override val name = "createTable" }
case object DeleteTable extends Action { override val name = "deleteTable" }
case object EditTableStructureProperty extends Action { override val name = "editTableStructureProperty" }
case object ViewHiddenTable extends Action { override val name = "viewHiddenTable" }
case object ViewColumn extends Action { override val name = "viewColumn" }
case object EditColumnDisplayProperty extends Action { override val name = "editColumnDisplayProperty" }
case object EditColumnStructureProperty extends Action { override val name = "editColumnStructureProperty" }
case object CreateColumn extends Action { override val name = "createColumn" }
case object DeleteColumn extends Action { override val name = "deleteColumn" }
case object ViewCellValue extends Action { override val name = "viewCellValue" }
case object EditCellValue extends Action { override val name = "editCellValue" }
case object CreateMedia extends Action { override val name = "createMedia" }
case object EditMedia extends Action { override val name = "editMedia" }
case object DeleteMedia extends Action { override val name = "deleteMedia" }
case object CreateTableGroup extends Action { override val name = "createTableGroup" }
case object EditTableGroup extends Action { override val name = "editTableGroup" }
case object DeleteTableGroup extends Action { override val name = "deleteTableGroup" }
case object CreateService extends Action { override val name = "createService" }
case object DeleteService extends Action { override val name = "deleteService" }
case object ViewService extends Action { override val name = "viewService" }
case object EditServiceDisplayProperty extends Action { override val name = "editServiceDisplayProperty" }
case object EditServiceStructureProperty extends Action { override val name = "editServiceStructureProperty" }
case object EditSystem extends Action { override val name = "editSystem" }
