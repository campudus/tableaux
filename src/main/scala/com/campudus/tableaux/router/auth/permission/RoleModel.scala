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

  def enrichDomainObject(
      inputJson: JsonObject,
      scope: Scope,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): JsonObject = {

    val grantPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Grant, scope)
    val denyPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Deny, scope)

    scope match {
      case ScopeMedia => enrichMediaObject(inputJson, grantPermissions, denyPermissions)
      case _ => ???
    }
  }

  def filterDomainObjects[A](
      scope: Scope,
      domainObjects: Seq[A],
      isSingleItemRequest: Boolean = false,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): Seq[A] = {

    val grantPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Grant, View, scope)
    val denyPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Deny, View, scope)

    val filteredObjects: Seq[A] = domainObjects.filter({ obj: A =>
      {

        val co: ComparisonObjects = obj match {
          case table: Table => ComparisonObjects(table)
          case _ => ??? // TODO
        }

        val matchingGrantPermission: Option[Permission] = grantPermissions.find(_.isMatching(co))

        val isAllowed: Boolean = matchingGrantPermission
          .map({
            grantPermission =>
              val matchingDenyPermission: Option[Permission] = denyPermissions.find(_.isMatching(co))

              matchingDenyPermission
                .map({ denyPermission =>
                  logger.debug(
                    s"DENY: A permission in role '${denyPermission.roleName}' filtered the item in scope '$scope'")
                  false
                })
                .getOrElse({
                  logger.debug(
                    s"GRANT: A permission in role '${grantPermission.roleName}' passed through the item in scope '$scope'")
                  true
                })
          })
          .getOrElse({
            logger.debug(s"DENY: No permission found that could pass through the item")
            false
          })

        isAllowed
      }
    })

    if (isSingleItemRequest && filteredObjects.isEmpty) {
      throw UnauthorizedException(View, scope)
    } else {
      filteredObjects
    }
  }

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
      action: Action,
      scope: Scope,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): Future[Unit] = {

    val grantPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Grant, action, scope)
    val denyPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Deny, action, scope)

    val matchingGrantPermission: Option[Permission] = grantPermissions.find(_.isMatching(objects))

    // TODO refactor
    val isAllowed: Boolean = matchingGrantPermission
      .map({ grantPermission =>
        val matchingDenyPermission: Option[Permission] = denyPermissions.find(_.isMatching(objects))

        matchingDenyPermission
          .map({ denyPermission =>
            logger.debug(s"DENY: A permission in role '${denyPermission.roleName}' denies access")
            false
          })
          .getOrElse({
            logger.debug(s"GRANT: A permission in role '${grantPermission.roleName}' grants access")
            true
          })
      })
      .getOrElse({
        logger.debug(s"DENY: No permission found that grants access")
        false
      })

    if (isAllowed) {
      Future.successful(())
    } else {
      Future.failed(UnauthorizedException(action, scope))
    }
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

  private def enrichMediaObject(
      inputJson: JsonObject,
      grantPermissions: Seq[Permission],
      denyPermissions: Seq[Permission]
  ): JsonObject = {

    // TODO refactor to reuse it in other enrich methods
    def isActionAllowed(action: Action): Boolean = {
      val matchingGrantPermission: Option[Permission] = grantPermissions.find(_.actions.contains(action))

      matchingGrantPermission
        .map({ grantPermission =>
          val matchingDenyPermission: Option[Permission] = denyPermissions.find(_.actions.contains(action))

          matchingDenyPermission
            .map({ denyPermission =>
              logger.debug(s"DENY: A permission in role '${denyPermission.roleName}' denies $action on Media")
              false
            })
            .getOrElse({
              logger.debug(s"GRANT: A permission in role '${grantPermission.roleName}' grants $action on Media")
              true
            })
        })
        .getOrElse({
          logger.debug(s"DENY: No permission found that grants $action on Media")
          false
        })
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
