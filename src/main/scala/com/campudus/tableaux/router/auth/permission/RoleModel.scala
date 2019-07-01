package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.domain.{ColumnType, Table}
import com.campudus.tableaux.{RequestContext, UnauthorizedException}
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
case class RoleModel(jsonObject: JsonObject) {

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
      filterObjects: Seq[A],
      isSingleItemRequest: Boolean = false,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit requestContext: RequestContext): Seq[A] = {

    val grantPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Grant, Some(View), scope)
    val denyPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Deny, Some(View), scope)

    val filteredObjects = filterObjects.filter({ obj: A =>
      {

        println()

        Console.println(s"XXX: $obj")

        val co = obj match {
          case table: Table => ComparisonObjects(table)
//          case column: ColumnType[_] => ComparisonObjects(column)
        }

        val isAllowed: Boolean = grantPermissions.exists(_.isMatching(co))
        val isForbidden: Boolean = denyPermissions.exists(_.isMatching(co))

        Console.println(s"XXX: $isAllowed $isForbidden")

        isAllowed && !isForbidden
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

//    Console.println(s"XXX: $requestRoles")
//    this.println()

    val grantPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Grant, Some(action), scope)
    val denyPermissions: Seq[Permission] = filterPermissions(requestContext.getUserRoles, Deny, Some(action), scope)

    // simples test working
    // TODO test with mixed grant and deny types
    val isAllowed: Boolean = grantPermissions.exists(_.isMatching(objects))
    val isForbidden: Boolean = denyPermissions.exists(_.isMatching(objects))

    if (isAllowed && !isForbidden) {
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

  private def enrichMediaObject(inputJson: JsonObject,
                                grantPermissions: Seq[Permission],
                                denyPermissions: Seq[Permission]): JsonObject = {

    val isCreateAllowed = grantPermissions.exists(_.actions.contains(Create))
    val isCreateForbidden = denyPermissions.exists(_.actions.contains(Create))

    val isEditAllowed = grantPermissions.exists(_.actions.contains(Edit))
    val isEditForbidden = denyPermissions.exists(_.actions.contains(Edit))

    val isDeleteAllowed = grantPermissions.exists(_.actions.contains(Delete))
    val isDeleteForbidden = denyPermissions.exists(_.actions.contains(Delete))

    inputJson.mergeIn(
      Json.obj(
        "permission" ->
          Json.obj(
            "create" -> (isCreateAllowed && !isCreateForbidden),
            "edit" -> (isEditAllowed && !isEditForbidden),
            "delete" -> (isDeleteAllowed && !isDeleteForbidden)
          ))
    )
  }

  // TODO possibly obsolete
  def getPermissionsForRoles(roleNames: Seq[String]): Seq[Permission] =
    role2permissions.filter({ case (key, _) => roleNames.contains(key) }).values.flatten.toSeq

  def filterPermissions(roleNames: Seq[String], permissionType: PermissionType, scope: Scope): Seq[Permission] =
    filterPermissions(roleNames, permissionType, None, scope)

  def filterPermissions(roleNames: Seq[String],
                        permissionType: PermissionType,
                        action: Action,
                        scope: Scope): Seq[Permission] =
    filterPermissions(roleNames, permissionType, Some(action), scope)

  /**
    * Filters permissions for role name, permissionType, action and scope
    *
    * @return a subset of permissions
    */
  private def filterPermissions(roleNames: Seq[String],
                                permissionType: PermissionType,
                                actionOpt: Option[Action],
                                scope: Scope): Seq[Permission] = {

    val permissions: Seq[Permission] =
      role2permissions.filter({ case (key, _) => roleNames.contains(key) }).values.flatten.toSeq

    permissions
      .filter(_.permissionType == permissionType)
      .filter(_.scope == scope)
      .filter(permission => actionOpt.forall(permission.actions.contains(_)))
  }

  def println(): Unit =
    role2permissions
      .foreach({
        case (key, permission) => Console.println(s"XXX: $key => ${permission.toString}")
      })
}
