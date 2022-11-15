package com.campudus.tableaux.router.auth.permission

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.database.{MultiCountry, MultiLanguage}
import com.campudus.tableaux.database.domain.{ColumnType, Service, Table}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.router.auth.KeycloakAuthHandler

import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future

import com.typesafe.scalalogging.LazyLogging

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
  *   - checkAuthorization: A check method for `POST`, `PUT`, `PATCH` und `DELETE` requests.
  *
  *   - filterDomainObjects: A filter method for `GET` requests, to only return viewable items. If a `GET` requests a
  *     specific resource, checkAuthorization should be called instead.
  *
  *   - enrichDomainObject: A enrich method for selected `GET` requests, to extend response items with permissions
  *     objects.
  *
  * History feature and recursive requests like deleteRow with delete cascade could trigger retrieve* methods that are
  * not allowed for a user. In this case we must mark these requests as internal so they are always granted
  * (isInternalCall: Boolean).
  */
class RoleModel(jsonObject: JsonObject) extends LazyLogging {

  /**
    * Checks if a writing request is allowed to change a resource. If not a UnauthorizedException is thrown.
    */
  def checkAuthorization(
      action: Action,
      objects: ComparisonObjects = ComparisonObjects(),
      isInternalCall: Boolean = false
  )(implicit user: TableauxUser): Future[Unit] = {

    if (isInternalCall) {
      Future.successful(())
    } else {
      val userRoles: Seq[String] = user.roles

      // In case action == ViewCellValue we must not check langtag conditions,
      //  because we can not retrieve cell values for one language.
      val withLangtagCondition: Boolean = action != ViewCellValue

      if (isAllowed(userRoles, action, _.isMatching(objects, withLangtagCondition), Check)) {
        Future.successful(())
      } else {
        val userName = user.name
        logger.info(s"User ${userName} is not allowed to do action '$action'")
        Future.failed(UnauthorizedException(action, userRoles))
      }
    }
  }

  /**
    * Filters the returning domainObjects of a response of a `GET` requests to only viewable items.
    *
    * For example, when filtering columns, it is also possible to filter them according to their table.
    */
  def filterDomainObjects[A](
      action: Action,
      domainObjects: Seq[A],
      parentObjects: ComparisonObjects = ComparisonObjects(),
      isInternalCall: Boolean
  )(implicit user: TableauxUser): Seq[A] = {

    if (isInternalCall) {
      domainObjects
    } else {
      val userRoles: Seq[String] = user.roles

      domainObjects.filter({ obj: A =>
        // for each domainObject generate objects to compare with
        // for media and service there's only a global view permission
        val objects: ComparisonObjects = obj match {
          case table: Table => ComparisonObjects(table)
          case column: ColumnType[_] => parentObjects.merge(column)
          case _: Service => ComparisonObjects()
          case _ => ComparisonObjects()
        }

        isAllowed(userRoles, action, _.isMatching(objects), Filter)
      })
    }
  }

  /**
    * Enriches a domainObject with a permission object to extend the response item with additional permission info.
    */
  def enrichDomainObject(
      inputJson: JsonObject,
      scope: Scope,
      objects: ComparisonObjects = ComparisonObjects()
  )(implicit user: TableauxUser): JsonObject = {

    val userRoles = user.roles

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
      isAllowed(userRoles, action, _.actions.contains(action), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "editDisplayProperty" -> isActionAllowed(EditServiceDisplayProperty),
      "editStructureProperty" -> isActionAllowed(EditServiceStructureProperty),
      "delete" -> isActionAllowed(DeleteService)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichTable(inputJson: JsonObject, userRoles: Seq[String], objects: ComparisonObjects): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, _.isMatching(objects), Enrich)
    }

    val permissionJson: JsonObject =
      Json.obj(
        "editTableDisplayProperty" -> isActionAllowed(EditTableDisplayProperty),
        "editTableStructureProperty" -> isActionAllowed(EditTableStructureProperty),
        "deleteTable" -> isActionAllowed(DeleteTable),
        "createRow" -> isActionAllowed(CreateRow),
        "deleteRow" -> isActionAllowed(DeleteRow),
        "editCellAnnotation" -> isActionAllowed(EditCellAnnotation),
        "editRowAnnotation" -> isActionAllowed(EditRowAnnotation)
      )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichServiceSeq(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {
    val permissionJson: JsonObject = Json.obj(
      "create" -> isAllowed(userRoles, CreateService, _.actions.contains(CreateService), Enrich)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichColumnSeq(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {
    val permissionJson: JsonObject = Json.obj(
      "create" -> isAllowed(userRoles, CreateColumn, _.actions.contains(CreateColumn), Enrich)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichColumn(inputJson: JsonObject, userRoles: Seq[String], objects: ComparisonObjects): JsonObject = {

    def getEditCellValuePermission: Any = {

      def getCellValueJson(langtags: Seq[String]) = {
        val editCellValueJson = Json.emptyObj()

        langtags.foreach(lt => {
          // generate dummy value for only this specific langtag
          val comparisonObjectsWithLangtagCheckValue = objects.merge(Json.obj(lt -> ""))

          editCellValueJson.mergeIn(
            Json.obj(
              lt ->
                isAllowed(
                  userRoles,
                  EditCellValue,
                  _.isMatching(comparisonObjectsWithLangtagCheckValue),
                  Enrich
                )
            )
          )
        })

        editCellValueJson
      }

      objects.columnOpt match {
        case Some(column) => {

          column.languageType match {
            case MultiLanguage => {
              val langtags = objects.tableOpt.map(_.langtags.getOrElse(Seq.empty[String])).getOrElse(Seq.empty[String])

              getCellValueJson(langtags)
            }

            case MultiCountry(countryCodes) => {
              getCellValueJson(countryCodes.codes)
            }

            case _ => isActionAllowed(EditCellValue)
          }
        }

        case _ =>
      }
    }

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, _.isMatching(objects), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "editDisplayProperty" -> isActionAllowed(EditColumnDisplayProperty),
      "editStructureProperty" -> isActionAllowed(EditColumnStructureProperty),
      "editCellValue" -> getEditCellValuePermission,
      "delete" -> isActionAllowed(DeleteColumn)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichTableSeq(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, _.actions.contains(action), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "create" -> isActionAllowed(CreateTable)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  private def enrichMedia(inputJson: JsonObject, userRoles: Seq[String]): JsonObject = {

    def isActionAllowed(action: Action): Boolean = {
      isAllowed(userRoles, action, _.actions.contains(action), Enrich)
    }

    val permissionJson: JsonObject = Json.obj(
      "create" -> isActionAllowed(CreateMedia),
      "edit" -> isActionAllowed(EditMedia),
      "delete" -> isActionAllowed(DeleteMedia)
    )

    mergePermissionJson(inputJson, permissionJson)
  }

  /**
    * Core function that checks whether an action is allowed or denied for given set of user roles. The return value is
    * a boolean.
    */
  private def isAllowed(
      userRoles: Seq[String],
      action: Action,
      function: Permission => Boolean,
      method: LoggingMethod
  ): Boolean = {

    def grantPermissions: Seq[Permission] = filterPermissions(userRoles, Grant, action)

    def denyPermissions: Seq[Permission] = filterPermissions(userRoles, Deny, action)

    val grantPermissionOpt: Option[Permission] = grantPermissions.find(function)

    grantPermissionOpt match {
      case Some(grantPermission) =>
        val denyPermissionOpt: Option[Permission] = denyPermissions.find(function)

        denyPermissionOpt match {
          case Some(denyPermission) => returnAndLog(Deny, loggingMessage(_, method, denyPermission, action))
          case None => returnAndLog(Grant, loggingMessage(_, method, grantPermission, action))
        }

      case None => returnAndLog(Deny, defaultLoggingMessage(_, method, userRoles, action))
    }
  }

  /**
    * Convenient method that combines two concerns.
    *   1. Logs a generated message with information which role matched with a permission 2. And depending on the
    *      PermissionType, it returns a Boolean result whether it allows or permits an action.
    */
  private def returnAndLog(permissionType: PermissionType, messageCurry: PermissionType => String): Boolean = {
    logger.debug(messageCurry(permissionType))
    permissionType.isAccessAllowed
  }

  private def defaultLoggingMessage(
      permissionType: PermissionType,
      method: LoggingMethod,
      userRoles: Seq[String],
      action: Action
  ): String =
    s"${permissionType.toString.toUpperCase}($method): No permission fitting for roles '$userRoles'. Action: '$action'"

  private def loggingMessage(
      permissionType: PermissionType,
      method: LoggingMethod,
      permission: Permission,
      action: Action
  ): String = {
    s"${permissionType.toString.toUpperCase}($method): A permission is fitting " +
      s"for role '${permission.roleName}'. Action: '$action'"
  }

  val role2permissions: Map[String, Seq[Permission]] =
    jsonObject
      .fieldNames()
      .asScala
      .map(roleName => {
        val permissionsJson: Seq[JsonObject] = asSeqOf[JsonObject](jsonObject.getJsonArray(roleName))
        (roleName, permissionsJson.map(permissionJson => Permission(permissionJson, roleName)))
      })
      .toMap

  override def toString: String =
    role2permissions
      .map({
        case (key, permission) => s"$key => ${permission.toString}"
      })
      .mkString("\n")

  private def getPermissionsForRoles(roleNames: Seq[String]): Seq[Permission] =
    role2permissions.filter({ case (key, _) => roleNames.contains(key) }).values.flatten.toSeq

  def filterPermissions(roleNames: Seq[String], permissionType: PermissionType): Seq[Permission] =
    filterPermissions(roleNames, Some(permissionType), None)

  def filterPermissions(
      roleNames: Seq[String],
      permissionType: PermissionType,
      action: Action
  ): Seq[Permission] =
    filterPermissions(roleNames, Some(permissionType), Some(action))

  /**
    * Filters permissions for role name, permissionType and action
    *
    * @return
    *   a subset of permissions
    */
  def filterPermissions(
      roleNames: Seq[String],
      permissionTypeOpt: Option[PermissionType] = None,
      actionOpt: Option[Action] = None
  ): Seq[Permission] = {

    val permissions: Seq[Permission] = getPermissionsForRoles(roleNames)

    permissions
      .filter(permission => permissionTypeOpt.forall(permission.permissionType == _))
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
      }
    )
    value
  }
}

/**
  * This class provides a legacy mode for downward compatibility purposes. If no authorization configuration is
  * specified, the service starts without verifying access tokens and without authorizing user roles and permissions.
  */
class RoleModelStub extends RoleModel(Json.emptyObj()) with LazyLogging {

  override def checkAuthorization(action: Action, objects: ComparisonObjects, isInternalCall: Boolean)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    Future.successful(())
  }

  override def enrichDomainObject(inputJson: JsonObject, scope: Scope, objects: ComparisonObjects)(
      implicit user: TableauxUser
  ): JsonObject = {
    inputJson
  }

  override def filterDomainObjects[A](
      action: Action,
      domainObjects: Seq[A],
      parentObjects: ComparisonObjects,
      isInternalCall: Boolean
  )(implicit user: TableauxUser): Seq[A] = {
    domainObjects
  }
}
