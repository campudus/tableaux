package com.campudus.tableaux.database.domain

import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json.{Json, JsonObject}

import org.joda.time.DateTime

object UserSettingKind {

  final val GLOBAL = "global"
  final val TABLE = "table"
  final val FILTER = "filter"
}

sealed trait UserSettingKind {

  def toString: String
}

case object UserSettingKindGlobal extends UserSettingKind {

  override def toString: String = UserSettingKind.GLOBAL
}

case object UserSettingKindTable extends UserSettingKind {

  override def toString: String = UserSettingKind.TABLE
}

case object UserSettingKindFilter extends UserSettingKind {

  override def toString: String = UserSettingKind.FILTER
}

case class UserSettingGlobal(
    key: String,
    value: Object, // json value (array, object, boolean)
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
)(implicit roleModel: RoleModel, user: TableauxUser) extends DomainObject {

  override def getJson: JsonObject = {
    val settingJson: JsonObject = Json.obj(
      "key" -> key,
      "value" -> value,
      "createdAt" -> optionToString(createdAt),
      "updatedAt" -> optionToString(updatedAt)
    )
    settingJson
  }
}

case class UserSettingGlobalSeq(settings: Seq[UserSettingGlobal])(implicit roleModel: RoleModel, user: TableauxUser)
    extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("settings" -> settings.map(_.getJson))
  }
}
