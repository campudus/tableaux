package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.helper.JsonUtils.asSeqOf
import org.vertx.scala.core.json.JsonObject

case class Permission(
    permissionType: PermissionType,
    actions: Seq[Action],
    scope: Scope,
    condition: ConditionContainer
) {

  def isMatching(subjects: ComparisonObjects): Boolean = {

    // TODO log which permission/role granted access
    scope match {
      case ScopeMedia => true
      case ScopeTable => condition.conditionTable.isMatching(subjects)
      case _ => ???
    }
  }
}

object Permission {

  def apply(jsonObject: JsonObject): Permission = {
    val permissionType: PermissionType = PermissionType(jsonObject.getString("type"))
    val actionString: Seq[String] = asSeqOf[String](jsonObject.getJsonArray("action"))
    val actions: Seq[Action] = actionString.map(key => Action(key))
    val scope: Scope = Scope(jsonObject.getString("scope"))
    val condition: ConditionContainer = ConditionContainer(jsonObject.getJsonObject("condition"))

    new Permission(permissionType, actions, scope, condition)
  }
}

sealed trait PermissionType

object PermissionType {

  def apply(permissionTypeString: String): PermissionType = {
    permissionTypeString match {
      case "grant" => Grant
      case "deny" => Deny
      case _ => throw new IllegalArgumentException(s"Invalid argument for PermissionType $permissionTypeString")
    }
  }
}

case object Grant extends PermissionType

case object Deny extends PermissionType
