package com.campudus.tableaux.database.model

import com.campudus.tableaux.InvalidUserSettingException
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.Try

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

  private def convertJsonArrayToUserSetting(arr: JsonArray)(implicit user: TableauxUser): UserSetting = {
    UserSetting(
      arr.get[String](0), // key
      arr.get[String](1), // kind
      Json.fromObjectString(arr.get[String](2)).getValue("value"), // value
      convertStringToDateTime(arr.get[String](3)), // created_at
      convertStringToDateTime(arr.get[String](4)), // updated_at
      Try(arr.getLong(5).longValue()).toOption, // tableId
      Try(arr.getString(6)).toOption, // name
      Try(arr.getLong(7).longValue()).toOption // id
    )
  }

  def checkSettingKey(settingKey: String)(implicit user: TableauxUser): Future[Unit] = {
    val select = "SELECT COUNT(*) = 1 FROM user_setting_schemas WHERE key = ?"

    connection.selectSingleValue[Boolean](select, Json.arr(settingKey)).flatMap({
      case true => Future.successful(())
      case false => Future.failed(InvalidUserSettingException(s"setting $settingKey doesn't exist."))
    })
  }

  def retrieveSettingKind(settingKey: String)(implicit user: TableauxUser): Future[String] = {
    val select = "SELECT kind FROM user_setting_schemas WHERE key = ?"

    for {
      result <- connection.selectSingleValue[String](select, Json.arr(settingKey))
    } yield {
      result
    }
  }

  def retrieveSettingSchema(settingKey: String)(implicit user: TableauxUser): Future[String] = {
    val select = "SELECT schema FROM user_setting_schemas WHERE key = ?"

    for {
      result <- connection.selectSingleValue[String](select, Json.arr(settingKey))
    } yield {
      result
    }
  }

  def retrieveSettings(kind: Option[String])(implicit user: TableauxUser): Future[Seq[UserSetting]] = {
    val (where, values) = kind match {
      case None => ("WHERE COALESCE(usg.user_id, ust.user_id, usf.user_id) = ?", Json.arr(user.name))
      case Some(value) =>
        ("WHERE COALESCE(usg.user_id, ust.user_id, usf.user_id) = ? AND kind = ?", Json.arr(user.name, value))
    }

    val select =
      s"""
         |SELECT
         |  uss.key,
         |  uss.kind,
         |  COALESCE(usg.value, ust.value, usf.value) AS value,
         |  COALESCE(usg.created_at, ust.created_at, usf.created_at) AS created_at,
         |  COALESCE(usg.updated_at, ust.updated_at, usf.updated_at) AS updated_at,
         |  ust.table_id,
         |  usf.name,
         |  usf.id
         |FROM
         |  user_setting_schemas uss
         |LEFT JOIN
         |  user_settings_global usg ON usg.key = uss.key
         |LEFT JOIN
         |  user_settings_table ust ON ust.key = uss.key
         |LEFT JOIN
         |  user_settings_filter usf ON usf.key = uss.key
         |$where
         |ORDER BY key, table_id, id
         """.stripMargin

    for {
      result <- connection.query(select, values)
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToUserSetting)
    }
  }

  def retrieveGlobalSetting(settingKey: String)(implicit user: TableauxUser): Future[UserSetting] = {
    val select =
      s"""
         |SELECT
         |  uss.key,
         |  uss.kind,
         |  usg.value,
         |  usg.created_at,
         |  usg.updated_at
         |FROM
         |  user_setting_schemas uss
         |LEFT JOIN
         |  user_settings_global usg ON usg.key = uss.key
         |WHERE
         |  usg.user_id = ?
         |AND
         |  uss.key = ?
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToUserSetting(resultArr.head)
    }
  }

  def upsertGlobalSetting(settingKey: String, settingValue: Object)(implicit
  user: TableauxUser): Future[UserSetting] = {
    for {
      result <- connection.query(
        s"""
           |INSERT INTO user_settings_global AS usg
           |  (key, value, user_id)
           |VALUES
           |  (?, ?, ?)
           |ON CONFLICT (key, user_id)
           |DO UPDATE SET
           |  value = EXCLUDED.value,
           |  updated_at = CURRENT_TIMESTAMP
           |WHERE
           |  usg.key = EXCLUDED.key
           |AND
           |  usg.user_id = EXCLUDED.user_id
           |RETURNING usg.key, usg.created_at
          """.stripMargin,
        Json.arr(settingKey, settingValue, user.name)
      )
      _ = insertNotNull(result)
      setting <- retrieveGlobalSetting(settingKey)
    } yield {
      setting
    }
  }

  def retrieveTableSetting(settingKey: String, tableId: Long)(implicit user: TableauxUser): Future[UserSetting] = {
    val select =
      s"""
         |SELECT
         |  uss.key,
         |  uss.kind,
         |  ust.value,
         |  ust.created_at,
         |  ust.updated_at,
         |  ust.table_id
         |FROM
         |  user_setting_schemas uss
         |LEFT JOIN
         |  user_settings_table ust ON ust.key = uss.key
         |WHERE
         |  ust.user_id = ?
         |AND
         |  uss.key = ?
         |AND
         |  ust.table_id = ?
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey, tableId))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToUserSetting(resultArr.head)
    }
  }

  def upsertTableSetting(settingKey: String, settingValue: Object, tableId: Long)(implicit
  user: TableauxUser): Future[UserSetting] = {
    for {
      result <- connection.query(
        s"""
           |INSERT INTO user_settings_table AS ust
           |  (key, value, user_id, table_id)
           |VALUES
           |  (?, ?, ?, ?)
           |ON CONFLICT (key, user_id, table_id)
           |DO UPDATE SET
           |  value = EXCLUDED.value,
           |  updated_at = CURRENT_TIMESTAMP
           |WHERE
           |  ust.key = EXCLUDED.key
           |AND
           |  ust.user_id = EXCLUDED.user_id
           |AND
           |  ust.table_id = EXCLUDED.table_id
           |RETURNING ust.key, ust.created_at
          """.stripMargin,
        Json.arr(settingKey, settingValue, user.name, tableId)
      )
      _ = insertNotNull(result)
      setting <- retrieveTableSetting(settingKey, tableId)
    } yield {
      setting
    }
  }

  def retrieveFilterSetting(settingKey: String, id: Long)(implicit user: TableauxUser): Future[UserSetting] = {
    val select =
      s"""
         |SELECT
         |  uss.key,
         |  uss.kind,
         |  usf.value,
         |  usf.created_at,
         |  usf.updated_at,
         |  null as table_id, -- ignore tableId column
         |  usf.name,
         |  usf.id
         |FROM
         |  user_setting_schemas uss
         |LEFT JOIN
         |  user_settings_filter usf ON usf.key = uss.key
         |WHERE
         |  usf.user_id = ?
         |AND
         |  uss.key = ?
         |AND
         |  usf.id = ?
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey, id))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToUserSetting(resultArr.head)
    }
  }

  def upsertFilterSetting(settingKey: String, settingValue: Object, settingName: String)(implicit
  user: TableauxUser): Future[UserSetting] = {
    for {
      result <- connection.query(
        s"""
           |INSERT INTO user_settings_filter
           |  (key, value, user_id, name)
           |VALUES
           |  (?, ?, ?, ?)
           |RETURNING id, created_at
          """.stripMargin,
        Json.arr(settingKey, settingValue, user.name, settingName)
      )
      id = insertNotNull(result).head.get[Long](0)
      setting <- retrieveFilterSetting(settingKey, id)
    } yield {
      setting
    }
  }
}
