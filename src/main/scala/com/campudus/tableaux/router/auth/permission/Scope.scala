package com.campudus.tableaux.router.auth.permission

sealed trait Scope {
  val name: String
  override def toString: String = name
}

object Scope {

  def apply(scope: String): Scope = {
    scope match {
      case ScopeTable.name => ScopeTable
      case ScopeColumn.name => ScopeColumn
      case ScopeRow.name => ScopeRow
      case ScopeCell.name => ScopeCell
      case ScopeMedia.name => ScopeMedia
      case _ => throw new IllegalArgumentException(s"Invalid argument for PermissionType $scope")
    }
  }
}

case object ScopeTable extends Scope { override val name = "table" }
case object ScopeColumn extends Scope { override val name = "column" }
case object ScopeRow extends Scope { override val name = "row" }
case object ScopeCell extends Scope { override val name = "cell" }
case object ScopeMedia extends Scope { override val name = "media" }
