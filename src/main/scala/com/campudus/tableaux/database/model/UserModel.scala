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
      arr.get[String](1), // value
      convertStringToDateTime(arr.get[String](2)), // created_at
      convertStringToDateTime(arr.get[String](3)) // updated_at
    )
  }

  def retrieveGlobalSettings()(implicit user: TableauxUser): Future[Seq[UserSettingGlobal]] = {
    val select =
      s"SELECT key, value, created_at, updated_at from user_settings_global WHERE user_id = ? ORDER BY key"
    for {
      result <- connection.query(select, Json.arr(user.name))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToUserSettingGlobal)
    }
  }
}
