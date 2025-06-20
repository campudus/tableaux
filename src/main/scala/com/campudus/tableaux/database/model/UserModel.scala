package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.database.domain.UserSettingGlobal
import com.campudus.tableaux.helper.ResultChecker.resultObjectToJsonArray
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json._

import scala.concurrent.Future

object UserModel {

  def apply(connection: DatabaseConnection)(
      implicit roleModel: RoleModel
  ): UserModel = {
    new UserModel(connection)
  }
}

class UserModel(override protected[this] val connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {

  private def convertJsonArrayToUserSettingGlobal(arr: JsonArray)(implicit user: TableauxUser): UserSettingGlobal = {
    UserSettingGlobal(
      arr.get[String](0), // key
      Json.fromObjectString(arr.get[String](1)).getValue("value"), // value
      convertStringToDateTime(arr.get[String](2)), // created_at
      convertStringToDateTime(arr.get[String](3)) // updated_at
    )
  }

  def retrieveSettingSchema(settingKey: String)(implicit user: TableauxUser): Future[String] = {
    val select =
      s"""
         |SELECT
         |  schema
         |FROM
         |  user_setting_schemas
         |WHERE
         |  key = ?
         """.stripMargin

    for {
      result <- connection.selectSingleValue[String](select, Json.arr(settingKey))
    } yield {
      result
    }
  }

  def retrieveGlobalSettings()(implicit user: TableauxUser): Future[Seq[UserSettingGlobal]] = {
    val select =
      s"""
         |SELECT
         |  key,
         |  value,
         |  created_at,
         |  updated_at
         |FROM
         |  user_settings_global
         |WHERE
         |  user_id = ?
         |ORDER BY key
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToUserSettingGlobal)
    }
  }

  def retrieveGlobalSetting(settingKey: String)(implicit user: TableauxUser): Future[UserSettingGlobal] = {
    val select =
      s"""
         |SELECT
         |  key,
         |  value,
         |  created_at,
         |  updated_at
         |FROM
         |  user_settings_global
         |WHERE
         |  user_id = ?
         |AND
         |  key = ?
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      convertJsonArrayToUserSettingGlobal(resultArr.head)
    }
  }

  def updateGlobalSetting(settingKey: String, settingValue: Object)(implicit
  user: TableauxUser): Future[Seq[UserSettingGlobal]] = {
    val update =
      s"""
         |UPDATE
         |  user_settings_global
         |SET
         |  value = ?,
         |  updated_at = CURRENT_TIMESTAMP
         |WHERE
         |  user_id = ?
         |AND
         |  key = ?
         """.stripMargin

    for {
      result <- connection.query(update, Json.arr(settingValue, user.name, settingKey))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToUserSettingGlobal)
    }
  }
}
