package com.campudus.tableaux.database.domain

import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json.{Json, JsonObject}

import org.joda.time.DateTime

case class UserSettingGlobal(
    key: String,
    value: String,
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
