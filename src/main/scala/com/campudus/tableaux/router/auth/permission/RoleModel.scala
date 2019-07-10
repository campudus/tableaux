package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.domain.{ColumnType, Table}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{RequestContext, UnauthorizedException}
import com.typesafe.scalalogging.LazyLogging
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future

object RoleModel {

  def apply(jsonObject: JsonObject, isAuthorization: Boolean = true): RoleModel = {
    if (isAuthorization) {
      new RoleModel(jsonObject)
    } else {
      new RoleModelStub()
    }
  }
}

/**
  * RoleModel is responsible for providing these main functions:
  *
  * - checkAuthorization:
  *       A check method for `POST`, `PUT`, `PATCH` und `DELETE` requests.
  *
  * - filterDomainObjects:
  *       A filter method for `GET` requests, to only return viewable items.
  *       If a `GET` requests a specific resource, checkAuthorization should be called instead.
  *
  * - enrichDomainObject:
  *       A enrich method for selected `GET` requests, to extend response items with permissions objects.
  */
class RoleModel(jsonObject: JsonObject) extends LazyLogging {

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

    // For action view condition "langtag" must not be considered.
    val withLangtagCondition: Boolean = action != View

    if (isAllowed(userRoles, action, scope, _.isMatching(objects, withLangtagCondition))) {
      Future.successful(())
    } else {
      Future.failed(UnauthorizedException(action, scope))
    }
  }

  /**
    * Filters the returning domainObjects of a response of
    * a `GET` requests to only viewable items.
    *
    * For example, when filtering columns, it is also possible to
    * filter them according to their table.
    */
  def filterDomainObjects[A](
      scope: Scope,
      domainObjects: Seq[A],
      parentObjects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): Seq[A] = {

    val userRoles: Seq[String] = requestContext.getUserRoles

    domainObjects.filter({ obj: A =>
      val objects: ComparisonObjects = obj match {
        case table: Table => ComparisonObjects(table)
        case column: ColumnType[_] => parentObjects.merge(column)
        case _ => ??? // TODO
      }

      isAllowed(userRoles, View, scope, _.isMatching(objects))
    })
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

  /**
    * Core function that checks whether an action in a scope is allowed or
    * denied for given set of user roles. The return value is a boolean.
    */
  private def isAllowed(
      userRoles: Seq[String],
      action: Action,
      scope: Scope,
      function: Permission => Boolean
  ): Boolean = {

    def grantPermissions: Seq[Permission] = filterPermissions(userRoles, Grant, action, scope)

    def denyPermissions: Seq[Permission] = filterPermissions(userRoles, Deny, action, scope)

    val grantPermissionOpt: Option[Permission] = grantPermissions.find(function)

    grantPermissionOpt match {
      case Some(grantPermission) =>
        val denyPermissionOpt: Option[Permission] = denyPermissions.find(function)

        denyPermissionOpt match {
          case Some(denyPermission) => returnAndLog(Deny, loggingMessage(_, denyPermission, scope, action))
          case None => returnAndLog(Grant, loggingMessage(_, grantPermission, scope, action))
        }

      case None => returnAndLog(Deny, defaultLoggingMessage(_, userRoles))
    }
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

  private def defaultLoggingMessage(permissionType: PermissionType, userRoles: Seq[String]): String =
    s"${permissionType.toString.toUpperCase}: No permission fitting for roles '$userRoles'"

  private def loggingMessage(
      permissionType: PermissionType,
      permission: Permission,
      scope: Scope,
      action: Action
  ): String = {
    s"${permissionType.toString.toUpperCase}: A permission is fitting " +
      s"for role '${permission.roleName}'. Scope: '$scope' Action: '$action'"
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

/**
  * This class provides a legacy mode for downward compatibility purposes.
  * If no authorization configuration is specified, the service starts without verifying
  * access tokens and without authorizing user roles and permissions.
  */
class RoleModelStub extends RoleModel(Json.emptyObj()) with LazyLogging {

  private def logAuthWarning(): Unit =
    logger.warn(
      "Security risk! The server runs in legacy mode without authentication and authorization! " +
        "Please run the service with an authorization configuration and user role permissions.")

  override def checkAuthorization(action: Action, scope: Scope, objects: ComparisonObjects)(
      implicit requestContext: RequestContext): Future[Unit] = {
    logAuthWarning()
    Future.successful(())
  }

  override def enrichDomainObject(inputJson: JsonObject, scope: Scope, objects: ComparisonObjects)(
      implicit requestContext: RequestContext): JsonObject = {
    logAuthWarning()
    inputJson
  }

  override def filterDomainObjects[A](scope: Scope, domainObjects: Seq[A], parentObjects: ComparisonObjects)(
      implicit requestContext: RequestContext): Seq[A] = {
    logAuthWarning()
    domainObjects
  }
}
