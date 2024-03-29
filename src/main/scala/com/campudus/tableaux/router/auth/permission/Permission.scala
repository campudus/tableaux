package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.helper.JsonUtils.asSeqOf

import org.vertx.scala.core.json.JsonObject

import com.typesafe.scalalogging.LazyLogging

case class Permission(
    roleName: String,
    permissionType: PermissionType,
    actions: Seq[Action],
    conditions: ConditionContainer
) extends LazyLogging {

  def isMatching(
      action: Action,
      objects: ComparisonObjects = ComparisonObjects()
  ): Boolean =
    conditions.isMatching(action, objects)
}

object Permission {

  def apply(jsonObject: JsonObject): Permission = this.apply(jsonObject, "")

  def apply(jsonObject: JsonObject, roleName: String): Permission = {
    val permissionType: PermissionType = PermissionType(jsonObject.getString("type"))
    val actionString: Seq[String] = asSeqOf[String](jsonObject.getJsonArray("action"))
    val actions: Seq[Action] = actionString.map(key => Action(key))
    val condition: ConditionContainer = ConditionContainer(Option(jsonObject.getJsonObject("condition")))

    new Permission(roleName, permissionType, actions, condition)
  }
}

sealed trait PermissionType {
  def isAccessAllowed: Boolean = false
}

object PermissionType {

  def apply(permissionTypeString: String): PermissionType = {
    permissionTypeString match {
      case "grant" => Grant
      case "deny" => Deny
      case _ => throw new IllegalArgumentException(s"Invalid argument for PermissionType $permissionTypeString")
    }
  }
}

case object Grant extends PermissionType {
  override def isAccessAllowed: Boolean = true
}

case object Deny extends PermissionType
