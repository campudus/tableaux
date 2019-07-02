package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.domain.{ColumnType, Table}
import com.campudus.tableaux.{RequestContext, UnauthorizedException}
import com.campudus.tableaux.helper.JsonUtils._
import com.typesafe.scalalogging.LazyLogging
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future

object RoleModel {

  def apply(jsonObject: JsonObject): RoleModel = {
    new RoleModel(jsonObject)
  }
}

/**
  * ## RoleModel is responsible for providing these main functions:
  *
  * - checkAuthorization:
  *       a check method for `POST`, `PUT`, `PATCH` und `DELETE` requests
  *
  * - filterDomainObjects:
  *       a filter method for `GET` requests, to only return viewable items
  *
  * - enrichDomainObject:
  *       a enrich method for selected `GET` requests, to extend response items with permissions objects
  */
case class RoleModel(jsonObject: JsonObject) extends LazyLogging {

  /**
    * Checks if a writing request is allowed to change a resource.
    * If not a UnauthorizedException is thrown.
    */
  def checkAuthorization(
      action: Action,
      scope: Scope,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): Future[Unit] = {

    val userRoles: Seq[String] = requestContext.getUserRoles

    if (isAllowed(userRoles, action, scope, _.isMatching(objects))) {
      Future.successful(())
    } else {
      Future.failed(UnauthorizedException(action, scope))
    }
  }

  /**
    * Filters the returning domainObjects of a response of
    * a `GET` requests to only viewable items.
    * If the `GET` requests points to a specific resource and there is no
    * permission that grants access a UnauthorizedException is thrown
    */
  def filterDomainObjects[A](
      scope: Scope,
      domainObjects: Seq[A],
      isSingleItemRequest: Boolean = false,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): Seq[A] = {

    val userRoles: Seq[String] = requestContext.getUserRoles

    val filteredObjects: Seq[A] = domainObjects.filter({ obj: A =>
      val co: ComparisonObjects = obj match {
        case table: Table => ComparisonObjects(table)
        case _ => ??? // TODO
      }

      isAllowed(userRoles, View, scope, _.isMatching(co))
    })

    if (isSingleItemRequest && filteredObjects.isEmpty) {
      throw UnauthorizedException(View, scope)
    } else {
      filteredObjects
    }
  }

  /**
    * Enriches a domainObject with a permission object to
    * extend the response item with additional permission info.
    */
  def enrichDomainObject(
      inputJson: JsonObject,
      scope: Scope,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): JsonObject = {

    val userRoles: Seq[String] = requestContext.getUserRoles

    scope match {
      case ScopeMedia => enrichMediaObject(inputJson, userRoles, scope)
//      case ScopeMedia => enrichMediaObject(inputJson, grantPermissions, denyPermissions)
      case _ => ???
    }
  }

  private def enrichMediaObject(
      inputJson: JsonObject,
      userRoles: Seq[String],
      scope: Scope
  ): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, scope, _.actions.contains(action))
    }

    inputJson.mergeIn(
      Json.obj(
        "permission" ->
          Json.obj(
            "create" -> isActionAllowed(Create),
            "edit" -> isActionAllowed(Edit),
            "delete" -> isActionAllowed(Delete)
          ))
    )
  }

  private def isAllowed(
      userRoles: Seq[String],
      action: Action,
      scope: Scope,
      function: Permission => Boolean
  ): Boolean = {

    def grantPermissions: Seq[Permission] = filterPermissions(userRoles, Grant, action, scope)
    def denyPermissions: Seq[Permission] = filterPermissions(userRoles, Deny, action, scope)

    val grantPermissionOpt: Option[Permission] = grantPermissions.find(function)

//    *  There are several steps
//    *    1. filter permissions for assigned user roles, desired action and scope
//      *    2. first check if a permission with type 'grant' meets the conditions
//    *        no condition means -> condition evaluates to 'true'
//    *    3. if at least one meets the conditions check also the deny types

    grantPermissionOpt
      .map({ grantPermission =>
        val denyPermissionOpt: Option[Permission] = denyPermissions.find(function)

        denyPermissionOpt
          .map(denyPermission => returnAndLog(Deny, loggingMessage(_, denyPermission, scope, View)))
          .getOrElse(returnAndLog(Grant, loggingMessage(_, grantPermission, scope, View)))
      })
      .getOrElse(returnAndLog(Deny, defaultLoggingMessage))
  }

  /**
    * Convenient method that combines two concerns.
    *   1. Logs a generated message with information which role matched with a permission
    *   2. And depending on the PermissionType, it returns a Boolean result whether it allows or permits an action.
    */
  private def returnAndLog(permissionType: PermissionType, messageCurry: PermissionType => String): Boolean = {
    logger.debug(messageCurry(permissionType))
    permissionType.isAccessAllowed
  }

  private def defaultLoggingMessage(permissionType: PermissionType): String = "No permission fitting"

  private def loggingMessage(
      permissionType: PermissionType,
      permission: Permission,
      scope: Scope,
      action: Action
  ): String = {
    s"${permissionType.toString.toUpperCase}: A permission is fitting for role '${permission.roleName}'. Scope: '$scope' Action: '$action'"
  }

  val role2permissions: Map[String, Seq[Permission]] =
    jsonObject
      .fieldNames()
      .asScala
      .map(
        roleName => {
          val permissionsJson: Seq[JsonObject] = asSeqOf[JsonObject](jsonObject.getJsonArray(roleName))
          (roleName, permissionsJson.map(permissionJson => Permission(permissionJson, roleName)))
        }
      )
      .toMap

  override def toString: String =
    role2permissions
      .map({
        case (key, permission) => s"$key => ${permission.toString}"
      })
      .mkString("\n")

  private def getPermissionsForRoles(roleNames: Seq[String]): Seq[Permission] =
    role2permissions.filter({ case (key, _) => roleNames.contains(key) }).values.flatten.toSeq

  def filterPermissions(roleNames: Seq[String], permissionType: PermissionType, scope: Scope): Seq[Permission] =
    filterPermissions(roleNames, Some(permissionType), None, Some(scope))

  def filterPermissions(roleNames: Seq[String],
                        permissionType: PermissionType,
                        action: Action,
                        scope: Scope): Seq[Permission] =
    filterPermissions(roleNames, Some(permissionType), Some(action), Some(scope))

  /**
    * Filters permissions for role name, permissionType, action and scope
    *
    * @return a subset of permissions
    */
  def filterPermissions(
      roleNames: Seq[String],
      permissionTypeOpt: Option[PermissionType] = None,
      actionOpt: Option[Action] = None,
      scopeOpt: Option[Scope] = None
  ): Seq[Permission] = {

    val permissions: Seq[Permission] = getPermissionsForRoles(roleNames)

    permissions
      .filter(permission => permissionTypeOpt.forall(permission.permissionType == _))
      .filter(permission => scopeOpt.forall(permission.scope == _))
      .filter(permission => actionOpt.forall(permission.actions.contains(_)))
  }

  // TODO just a helper, delete after auth is implemented
  def printRolePermissions(): Unit =
    role2permissions
      .foreach({
        case (key, permission) => Console.println(s"XXX: $key => ${permission.toString}")
      })
}
