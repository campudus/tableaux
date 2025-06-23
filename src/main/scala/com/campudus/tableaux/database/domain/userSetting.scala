package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json.{Json, JsonObject}

import org.joda.time.DateTime

object UserSettingKind {

  final val GLOBAL = "global"
  final val TABLE = "table"
  final val FILTER = "filter"
}

case class UserSetting(
    key: String,
    kind: String,
    value: Object, // json value (array, object, boolean)
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime],
    tableId: Option[Long],
    name: Option[String],
    id: Option[Long]
)(implicit roleModel: RoleModel, user: TableauxUser) extends DomainObject {

  override def getJson: JsonObject = {
    val settingJson: JsonObject = Json.obj(
      "key" -> key,
      "kind" -> kind,
      "value" -> value
    )

    if (createdAt.isDefined) {
      settingJson.mergeIn(Json.obj("createdAt" -> optionToString(createdAt)))
    }

    if (updatedAt.isDefined) {
      settingJson.mergeIn(Json.obj("updatedAt" -> optionToString(updatedAt)))
    }

    if (tableId.isDefined) {
      settingJson.mergeIn(Json.obj("tableId" -> tableId.get))
    }

    if (name.isDefined) {
      settingJson.mergeIn(Json.obj("name" -> name.get))
    }

    if (id.isDefined) {
      settingJson.mergeIn(Json.obj("id" -> id.get))
    }

    settingJson
  }
}

case class UserSettingSeq(settings: Seq[UserSetting])(implicit roleModel: RoleModel, user: TableauxUser)
    extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj("settings" -> settings.map(_.getJson))
  }
}
