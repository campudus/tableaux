package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import org.joda.time.DateTime

sealed trait UserSettingKey {
  val name: String
}

sealed trait UserSettingKeyGlobal extends UserSettingKey
sealed trait UserSettingKeyTable extends UserSettingKey
sealed trait UserSettingKeyFilter extends UserSettingKey

object UserSettingKeyGlobal {

  case object FilterReset extends UserSettingKeyGlobal { val name = "filterReset" }
  case object SortingReset extends UserSettingKeyGlobal { val name = "sortingReset" }
  case object SortingDesc extends UserSettingKeyGlobal { val name = "sortingDesc" }
  case object AnnotationReset extends UserSettingKeyGlobal { val name = "annotationReset" }
  case object ColumnsReset extends UserSettingKeyGlobal { val name = "columnsReset" }
  case object MarkdownEditor extends UserSettingKeyGlobal { val name = "markdownEditor" }

  val keys: Set[UserSettingKeyGlobal] =
    Set(
      FilterReset,
      SortingReset,
      SortingDesc,
      AnnotationReset,
      ColumnsReset,
      MarkdownEditor
    )

  def fromKey(key: String): Option[UserSettingKeyGlobal] =
    keys.find(_.name == key)

  def isGlobalKey(key: String): Boolean =
    fromKey(key).isDefined
}

object UserSettingKeyTable {

  case object AnnotationHighlight extends UserSettingKeyTable { val name = "annotationHighlight" }
  case object ColumnOrdering extends UserSettingKeyTable { val name = "columnOrdering" }
  case object ColumnWidths extends UserSettingKeyTable { val name = "columnWidths" }
  case object RowsFilter extends UserSettingKeyTable { val name = "rowsFilter" }
  case object VisibleColumns extends UserSettingKeyTable { val name = "visibleColumns" }

  val keys: Set[UserSettingKeyTable] = Set(
    AnnotationHighlight,
    ColumnOrdering,
    ColumnWidths,
    RowsFilter,
    VisibleColumns
  )

  def fromKey(key: String): Option[UserSettingKeyTable] =
    keys.find(_.name == key)

  def isTableKey(key: String): Boolean =
    fromKey(key).isDefined
}

object UserSettingKeyFilter {

  case object PresetFilter extends UserSettingKeyFilter { val name = "presetFilter" }

  val keys: Set[UserSettingKeyFilter] = Set(PresetFilter)

  def fromKey(key: String): Option[UserSettingKeyFilter] =
    keys.find(_.name == key)

  def isFilterKey(key: String): Boolean =
    fromKey(key).isDefined
}

sealed trait UserSettingKind {
  val name: String
}

object UserSettingKind {

  def apply(kind: String): UserSettingKind = {
    kind match {
      case UserSettingKindGlobal.name => UserSettingKindGlobal
      case UserSettingKindTable.name => UserSettingKindTable
      case UserSettingKindFilter.name => UserSettingKindFilter
    }
  }
}

case object UserSettingKindGlobal extends UserSettingKind { val name = "global" }
case object UserSettingKindTable extends UserSettingKind { val name = "table" }
case object UserSettingKindFilter extends UserSettingKind { val name = "filter" }

sealed abstract class UserSetting[T](
    key: UserSettingKey,
    value: T,
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
) extends DomainObject {
  val kind: UserSettingKind

  def getJson: JsonObject = {
    val settingJson = Json.obj(
      "key" -> key.name,
      "kind" -> kind.name,
      "value" -> value
    )

    if (createdAt.isDefined) {
      settingJson.mergeIn(Json.obj("createdAt" -> optionToString(createdAt)))
    }

    if (updatedAt.isDefined) {
      settingJson.mergeIn(Json.obj("updatedAt" -> optionToString(updatedAt)))
    }

    settingJson
  }
}

case class UserSettingGlobal[T](
    key: UserSettingKeyGlobal,
    value: T,
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
) extends UserSetting[T](key, value, createdAt, updatedAt) {
  override val kind = UserSettingKindGlobal
}

case class UserSettingTable[T](
    tableId: Long,
    key: UserSettingKeyTable,
    value: T,
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
) extends UserSetting[T](key, value, createdAt, updatedAt) {
  override val kind = UserSettingKindTable

  override def getJson: JsonObject = {
    super.getJson.mergeIn(Json.obj("tableId" -> tableId))
  }
}

case class UserSettingFilter[T](
    id: Long,
    name: String,
    key: UserSettingKeyFilter,
    value: T,
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
) extends UserSetting[T](key, value, createdAt, updatedAt) {
  override val kind = UserSettingKindFilter

  override def getJson: JsonObject = {
    super.getJson.mergeIn(Json.obj("id" -> id, "name" -> name))
  }
}

case class UserSettingSeq(
    settings: Seq[UserSetting[_]]
)(implicit roleModel: RoleModel, user: TableauxUser)
    extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("settings" -> settings.map(_.getJson))
  }
}
