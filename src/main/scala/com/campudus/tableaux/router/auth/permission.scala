package com.campudus.tableaux.router.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.database.domain.{ColumnType, Table}
import com.campudus.tableaux.helper.JsonUtils.asSeqOf
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._
import scala.concurrent.Future

object RoleModel {

  def apply(jsonObject: JsonObject): RoleModel = {
    new RoleModel(jsonObject)
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
      tableOpt: Option[Table]
  ): Future[Unit] = {

    val grantPermissions: Seq[Permission] = filterPermissions(requestRoles, Grant, action, scope)

    val isAllowed: Boolean = grantPermissions.exists(_.isMatching(tableOpt))

//    Console.println(s"XXX $isAllowed")

    if (!isAllowed) {
      return Future.failed(UnauthorizedException(action, scope))
    }

    // simples test working
    // TODO test with mixed grant and deny types

    if (isAllowed) {
      val denyPermissions: Seq[Permission] = filterPermissions(requestRoles, Deny, action, scope)
      val isDenied: Boolean = denyPermissions.exists(_.isMatching(tableOpt))

      if (isDenied) {
        return Future.failed(UnauthorizedException(action, scope))
      }
    }

    Future.successful(())
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
    role2permissions.filter({ case (k, _) => roleNames.contains(k) }).values.flatten.toSeq

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
      role2permissions.filter({ case (k, _) => roleNames.contains(k) }).values.flatten.toSeq

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

case class Permission(
    permissionType: PermissionType,
    actions: Seq[Action],
    scope: Scope,
    condition: ConditionContainer
) {

  def isMatching(
      tableOpt: Option[Table] = None,
      columnOpt: Option[ColumnType[_]] = None
//    langtagOpt: Option[Table] = None
  ): Boolean = {

    // TODO log which permission/role granted access

    val isTableMatching = tableOpt match {
      case Some(table) => condition.conditionTable.isMatching(table)
      case None => false

    }

    isTableMatching
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

sealed trait Action {
  val name: String
  override def toString: String = name
}

object Action {

  def apply(action: String): Action = {
    action match {
      case Create.name => Create
      case View.name => View
      case Edit.name => Edit
      case Delete.name => Delete
      case EditDisplayProperty.name => EditDisplayProperty
      case CreateRow.name => CreateRow
      case DeleteRow.name => DeleteRow
      case EditStructureProperty.name => EditStructureProperty
      case EditCellAnnotation.name => EditCellAnnotation
      case EditRowAnnotation.name => EditRowAnnotation
      case ViewCellValue.name => ViewCellValue
      case EditCellValue.name => EditCellValue
      case _ => throw new IllegalArgumentException(s"Invalid argument for PermissionType $action")
    }
  }
}

case object Create extends Action { override val name = "create" }
case object View extends Action { override val name = "view" }
case object Edit extends Action { override val name = "edit" } // only for media
case object Delete extends Action { override val name = "delete" }
case object EditDisplayProperty extends Action { override val name = "editDisplayProperty" }
case object CreateRow extends Action { override val name = "createRow" }
case object DeleteRow extends Action { override val name = "deleteRow" }
case object EditStructureProperty extends Action { override val name = "editStructureProperty" }
case object EditCellAnnotation extends Action { override val name = "editCellAnnotation" }
case object EditRowAnnotation extends Action { override val name = "editRowAnnotation" }
case object ViewCellValue extends Action { override val name = "viewCellValue" }
case object EditCellValue extends Action { override val name = "editCellValue" }

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

object ConditionContainer {

  def apply(jsonObjectOrNull: JsonObject): ConditionContainer = {

    val jsonObject: JsonObject = Option(jsonObjectOrNull).getOrElse(Json.emptyObj())

    val ct: ConditionOption = Option(jsonObject.getJsonObject("table")).map(ConditionTable).getOrElse(NoneCondition)
    val cc: ConditionOption = Option(jsonObject.getJsonObject("column")).map(ConditionColumn).getOrElse(NoneCondition)
    val cl: ConditionOption =
      Option(Json.obj("langtag" -> jsonObject.getString("langtag"))).map(ConditionLangtag).getOrElse(NoneCondition)

    new ConditionContainer(ct, cc, cl)
  }
}

case class ConditionContainer(conditionTable: ConditionOption,
                              conditionColumn: ConditionOption,
                              conditionLangtag: ConditionOption) {}

abstract class ConditionOption(jsonObject: JsonObject) {
  val conditionMap: Map[String, String] = toMap(jsonObject)

  protected def toMap(jsonObject: JsonObject) = {
    jsonObject.asMap.toMap.asInstanceOf[Map[String, String]]
  }

  // TODO remove ugly method overloading, but how?
  def isMatching(table: Table) = false
  def isMatching(column: ColumnType[_]) = false
  def isMatching(langtag: String) = false
}

case class ConditionTable(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  override def isMatching(table: Table): Boolean = {
    conditionMap.forall({
      case (property, regex) => {

        property match {
          case "id" => table.id.toString.matches(regex)
          case "name" => table.name.matches(regex)
          case "hidden" => table.hidden.toString.matches(regex)
          case "tableType" => table.tableType.NAME.matches(regex)
          case "tableGroup" => table.tableGroup.exists(_.id.toString.matches(regex))
          case _ => false
        }
      }
    })
  }
}

case class ConditionColumn(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  // TODO implement regex validation for all possible fields
  //  - description
  //  - displayName
  //  - id
  //  - identifier
  //  - kind
  //  - multilanguage
  //  - name
  //  - ordering
}
case class ConditionLangtag(jsonObject: JsonObject) extends ConditionOption(jsonObject) {

  // TODO implement regex validation
}

case object NoneCondition extends ConditionOption(Json.emptyObj()) {
  override def isMatching(table: Table) = true
  override def isMatching(column: ColumnType[_]) = true
  override def isMatching(langtag: String) = true
}
