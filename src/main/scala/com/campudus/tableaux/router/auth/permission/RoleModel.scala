package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.database.LanguageNeutral
import com.campudus.tableaux.database.domain.{ColumnType, Service, Table}
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

  def apply(): RoleModel = new RoleModel(Json.emptyObj())
}

sealed trait LoggingMethod
case object Check extends LoggingMethod
case object Filter extends LoggingMethod
case object Enrich extends LoggingMethod

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
  *
  *
  * History feature and recursive requests like deleteRow with delete cascade could
  * trigger retrieve* methods that are not allowed for a user. In this case we must mark these
  * requests as internal so they are always granted (isInternalCall: Boolean).
  *
  */
class RoleModel(jsonObject: JsonObject) extends LazyLogging {

  /**
    * Checks if a writing request is allowed to change a resource.
    * If not a UnauthorizedException is thrown.
    */
  def checkAuthorization(
      action: Action,
      scope: Scope,
      objects: ComparisonObjects = ComparisonObjects(),
      isInternalCall: Boolean = false
  )(implicit requestContext: RequestContext): Future[Unit] = {

    if (isInternalCall) {
      Future.successful(())
    } else {
      val userRoles: Seq[String] = requestContext.getUserRoles

      // In case action == ViewCellValue we must not check langtag conditions,
      //  because we can not retrieve cell values for one language.
      val withLangtagCondition: Boolean = action != ViewCellValue

      if (isAllowed(userRoles, action, scope, _.isMatching(objects, withLangtagCondition), Check)) {
        Future.successful(())
      } else {
        Future.failed(UnauthorizedException(action, scope))
      }
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
      parentObjects: ComparisonObjects = ComparisonObjects(),
      isInternalCall: Boolean,
      action: Action = View
  )(implicit requestContext: RequestContext): Seq[A] = {

    if (isInternalCall) {
      domainObjects
    } else {
      val userRoles: Seq[String] = requestContext.getUserRoles

      domainObjects.filter({ obj: A =>
        // for each domainObject generate objects to compare with
        // for media and service there's only a global view permission
        val objects: ComparisonObjects = obj match {
          case table: Table => ComparisonObjects(table)
          case column: ColumnType[_] => parentObjects.merge(column)
          case _: Service => ComparisonObjects()
          case _ => ComparisonObjects()
        }

        isAllowed(userRoles, action, scope, _.isMatching(objects), Filter)
      })
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
      case ScopeMedia => enrichMedia(inputJson, userRoles)
      case ScopeTableSeq => enrichTableSeq(inputJson, userRoles)
      case ScopeTable => enrichTable(inputJson, userRoles, objects)
      case ScopeColumn => enrichColumn(inputJson, userRoles, objects)
      case ScopeColumnSeq => enrichColumnSeq(inputJson, userRoles)
      case ScopeService => enrichService(inputJson, userRoles)
      case ScopeServiceSeq => enrichServiceSeq(inputJson, userRoles)
      case _ => inputJson
    }
  }

  private def mergePermissionJson(inputJson: JsonObject, permissionJson: JsonObject): JsonObject =
    inputJson.mergeIn(Json.obj("permission" -> permissionJson))

  private def enrichService(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, ScopeService, _.actions.contains(action), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "editDisplayProperty" -> isActionAllowed(EditDisplayProperty),
      "editStructureProperty" -> isActionAllowed(EditStructureProperty),
      "delete" -> isActionAllowed(Delete)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichTable(inputJson: JsonObject, userRoles: Seq[String], objects: ComparisonObjects): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, ScopeTable, _.isMatching(objects), Enrich)
    }

    val permissionJson: JsonObject =
      Json.obj(
        "editDisplayProperty" -> isActionAllowed(EditDisplayProperty),
        "editStructureProperty" -> isActionAllowed(EditStructureProperty),
        "delete" -> isActionAllowed(Delete),
        "createRow" -> isActionAllowed(CreateRow),
        "deleteRow" -> isActionAllowed(DeleteRow),
        "editCellAnnotation" -> isActionAllowed(EditCellAnnotation),
        "editRowAnnotation" -> isActionAllowed(EditRowAnnotation)
      )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichServiceSeq(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {
    val permissionJson: JsonObject = Json.obj(
      "create" -> isAllowed(userRoles, Create, ScopeService, _.actions.contains(Create), Enrich)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichColumnSeq(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {
    val permissionJson: JsonObject = Json.obj(
      "create" -> isAllowed(userRoles, Create, ScopeColumn, _.actions.contains(Create), Enrich)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichColumn(inputJson: JsonObject, userRoles: Seq[String], objects: ComparisonObjects): JsonObject = {

    def getEditCellValuePermission: Any = {

      def isMultilanguage: Boolean = {
        objects.columnOpt.exists(_.languageType != LanguageNeutral)
      }

      if (isMultilanguage) {
        val langtags = objects.tableOpt.map(_.langtags.getOrElse(Seq.empty[String])).getOrElse(Seq.empty[String])

        val editCellValueJson = Json.emptyObj()

        langtags.foreach(lt => {
          // generate dummy value for only this specific langtag
          val comparisonObjectsWithLangtagCheckValue = objects.merge(Json.obj(lt -> ""))

          editCellValueJson.mergeIn(
            Json.obj(
              lt ->
                isAllowed(userRoles,
                          EditCellValue,
                          ScopeColumn,
                          _.isMatching(comparisonObjectsWithLangtagCheckValue),
                          Enrich)))
        })

        editCellValueJson

      } else {
        isActionAllowed(EditCellValue)
      }
    }

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, ScopeColumn, _.isMatching(objects), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "editDisplayProperty" -> isActionAllowed(EditDisplayProperty),
      "editStructureProperty" -> isActionAllowed(EditStructureProperty),
      "editCellValue" -> getEditCellValuePermission,
      "delete" -> isActionAllowed(Delete)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichTableSeq(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, ScopeTable, _.actions.contains(action), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "create" -> isActionAllowed(Create)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichMedia(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, ScopeMedia, _.actions.contains(action), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "create" -> isActionAllowed(Create),
      "edit" -> isActionAllowed(Edit),
      "delete" -> isActionAllowed(Delete)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  /**
    * Core function that checks whether an action in a scope is allowed or
    * denied for given set of user roles. The return value is a boolean.
    */
  private def isAllowed(
      userRoles: Seq[String],
      action: Action,
      scope: Scope,
      function: Permission => Boolean,
      method: LoggingMethod
  ): Boolean = {

    def grantPermissions: Seq[Permission] = filterPermissions(userRoles, Grant, action, scope)

    def denyPermissions: Seq[Permission] = filterPermissions(userRoles, Deny, action, scope)

    val grantPermissionOpt: Option[Permission] = grantPermissions.find(function)

    grantPermissionOpt match {
      case Some(grantPermission) =>
        val denyPermissionOpt: Option[Permission] = denyPermissions.find(function)

        denyPermissionOpt match {
          case Some(denyPermission) => returnAndLog(Deny, loggingMessage(_, method, denyPermission, scope, action))
          case None => returnAndLog(Grant, loggingMessage(_, method, grantPermission, scope, action))
        }

      case None => returnAndLog(Deny, defaultLoggingMessage(_, method, userRoles, scope, action))
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

  private def defaultLoggingMessage(
      permissionType: PermissionType,
      method: LoggingMethod,
      userRoles: Seq[String],
      scope: Scope,
      action: Action
  ): String =
    s"${permissionType.toString.toUpperCase}($method): No permission fitting for roles '$userRoles'. Scope: '$scope' Action: '$action'"

  private def loggingMessage(
      permissionType: PermissionType,
      method: LoggingMethod,
      permission: Permission,
      scope: Scope,
      action: Action
  ): String = {
    s"${permissionType.toString.toUpperCase}($method): A permission is fitting " +
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

  /* For some methods (clearCell, replaceCell), it is necessary to check the authorization on the basis of
   * an entire cell and not only on one or more specific langtags. In this case, we simply create a dummy value
   * object and enrich it with all configured languages (table or system). This way `checkAuthorization`
   * automatically checks for all languages.
   */
  def generateLangtagCheckValue(table: Table, value: JsonObject = Json.emptyObj()): JsonObject = {
    val langtags: Seq[String] = table.langtags.getOrElse(Seq.empty[String])

    langtags.foreach(lt =>
      if (!value.containsKey(lt)) {
        value.mergeIn(Json.obj(lt -> ""))
    })
    value
  }
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

  override def checkAuthorization(action: Action, scope: Scope, objects: ComparisonObjects, isInternalCall: Boolean)(
      implicit requestContext: RequestContext): Future[Unit] = {
    logAuthWarning()
    Future.successful(())
  }

  override def enrichDomainObject(inputJson: JsonObject, scope: Scope, objects: ComparisonObjects)(
      implicit requestContext: RequestContext): JsonObject = {
    logAuthWarning()
    inputJson
  }

  override def filterDomainObjects[A](
      scope: Scope,
      domainObjects: Seq[A],
      parentObjects: ComparisonObjects,
      isInternalCall: Boolean,
      action: Action = View
  )(implicit requestContext: RequestContext): Seq[A] = {
    logAuthWarning()
    domainObjects
  }
}
