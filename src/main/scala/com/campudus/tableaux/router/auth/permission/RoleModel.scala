package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.helper.JsonUtils._
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future

object RoleModel {

  def apply(jsonObject: JsonObject): RoleModel = {
    new RoleModel(jsonObject)
  }

  def apply(jsonObjectString: String): RoleModel = {
    new RoleModel(Json.fromObjectString(jsonObjectString))
  }
}

case class RoleModel(jsonObject: JsonObject) {

  /**
    * Checks the RoleModel if a specific action on a scope is allowed for a set of given roles.
    * If not a ForbiddenException is thrown
    *
    *  There are several steps
    *    1. filter permissions for assigned user roles, desired action and scope
    *    2. first check if a permission with type 'grant' meets the conditions
    *        no condition means -> condition evaluates to 'true'
    *
    *    3. if at least one meets the conditions check also the deny types
    *
    * @return
    */
  def checkAuthorization(
      requestRoles: Seq[String],
      action: Action,
      scope: Scope,
      subjects: ComparisonObjects = ComparisonObjects()
  ): Future[Unit] = {

//    Console.println(s"XXX: $requestRoles")
//    this.println()

    val grantPermissions: Seq[Permission] = filterPermissions(requestRoles, Grant, action, scope)

    val isAllowed: Boolean = grantPermissions.exists(_.isMatching(subjects))

    // simples test working
    // TODO test with mixed grant and deny types

    def fail = {
      Future.failed(UnauthorizedException(action, scope))
    }

    if (!isAllowed) {
      fail
    } else {
      val denyPermissions: Seq[Permission] = filterPermissions(requestRoles, Deny, action, scope)
      val isDenied: Boolean = denyPermissions.exists(_.isMatching(subjects))

      if (isDenied) {
        fail
      } else {
        Future.successful(())
      }
    }
  }

  val role2permissions: Map[String, Seq[Permission]] =
    jsonObject
      .fieldNames()
      .asScala
      .map(
        key => {
          val permissionsJson: Seq[JsonObject] = asSeqOf[JsonObject](jsonObject.getJsonArray(key))
          (key, permissionsJson.map(permissionJson => Permission(permissionJson)))
        }
      )
      .toMap

  override def toString: String =
    role2permissions
      .map({
        case (key, permission) => s"$key => ${permission.toString}"
      })
      .mkString("\n")

  // TODO possibly obsolete
  def getPermissionsForRoles(roleNames: Seq[String]): Seq[Permission] =
    role2permissions.filter({ case (key, _) => roleNames.contains(key) }).values.flatten.toSeq

  /**
    * Filters permissions for role name, permissionType, action and scope
    *
    * @return a subset of permissions
    */
  def filterPermissions(roleNames: Seq[String],
                        permissionType: PermissionType,
                        action: Action,
                        scope: Scope): Seq[Permission] = {

    val permissions: Seq[Permission] =
      role2permissions.filter({ case (key, _) => roleNames.contains(key) }).values.flatten.toSeq

    permissions
      .filter(_.permissionType == permissionType)
      .filter(_.scope == scope)
      .filter(_.actions.contains(action))
  }

  def println(): Unit =
    role2permissions
      .foreach({
        case (key, permission) => Console.println(s"XXX: $key => ${permission.toString}")
      })
}
