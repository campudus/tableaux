package com.campudus.tableaux.database.model

import com.campudus.tableaux.InvalidUserSettingException
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.helper.JsonUtils
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

  private def parseJsonString(value: String): Object = {
    Json.fromObjectString(s"""{"value":${value}}""").getValue("value")
  }

  private def convertJsonArrayToUserSettingGlobal(arr: JsonArray)(implicit user: TableauxUser): UserSettingGlobal[_] = {
    UserSettingGlobal(
      UserSettingKeyGlobal.fromKey(arr.get[String](0)).get, // key
      parseJsonString(arr.getString(1)), // value
      convertStringToDateTime(arr.get[String](2)), // created_at
      convertStringToDateTime(arr.get[String](3)) // updated_at
    )
  }

  private def convertJsonArrayToUserSettingTable(arr: JsonArray)(implicit user: TableauxUser): UserSettingTable[_] = {
    UserSettingTable(
      UserSettingKeyTable.fromKey(arr.get[String](0)).get, // key
      parseJsonString(arr.getString(1)), // value
      convertStringToDateTime(arr.get[String](2)), // created_at
      convertStringToDateTime(arr.get[String](3)), // updated_at
      arr.getLong(4).longValue() // tableId
    )
  }

  private def convertJsonArrayToUserSettingFilter(arr: JsonArray)(implicit user: TableauxUser): UserSettingFilter[_] = {
    UserSettingFilter(
      UserSettingKeyFilter.fromKey(arr.get[String](0)).get, // key
      parseJsonString(arr.getString(1)), // value
      convertStringToDateTime(arr.get[String](2)), // created_at
      convertStringToDateTime(arr.get[String](3)), // updated_at
      arr.getString(4), // name
      arr.getLong(5).longValue() // id
    )
  }

  def retrieveGlobalSetting(settingKey: String)(implicit user: TableauxUser): Future[UserSettingGlobal[_]] = {
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
         |ORDER BY key
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToUserSettingGlobal(resultArr.head)
    }
  }

  def retrieveGlobalSettings()(implicit user: TableauxUser): Future[Seq[UserSettingGlobal[_]]] = {
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

  def retrieveTableSetting(
      settingKey: String,
      tableId: Long
  )(implicit user: TableauxUser): Future[UserSettingTable[_]] = {
    val select =
      s"""
         |SELECT
         |  key,
         |  value,
         |  created_at,
         |  updated_at,
         |  table_id
         |FROM
         |  user_settings_table
         |WHERE
         |  user_id = ?
         |AND
         |  key = ?
         |AND
         |  table_id = ?
         |ORDER BY table_id, key
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey, tableId))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToUserSettingTable(resultArr.head)
    }
  }

  def retrieveTableSettings()(implicit user: TableauxUser): Future[Seq[UserSettingTable[_]]] = {
    val select =
      s"""
         |SELECT
         |  key,
         |  value,
         |  created_at,
         |  updated_at,
         |  table_id
         |FROM
         |  user_settings_table
         |WHERE
         |  user_id = ?
         |ORDER BY table_id, key
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToUserSettingTable)
    }
  }

  def retrieveFilterSetting(
      settingKey: String,
      id: Long
  )(implicit user: TableauxUser): Future[UserSettingFilter[_]] = {
    val select =
      s"""
         |SELECT
         |  key,
         |  value,
         |  created_at,
         |  updated_at,
         |  name,
         |  id
         |FROM
         |  user_settings_filter
         |WHERE
         |  user_id = ?
         |AND
         |  key = ?
         |AND
         |  id = ?
         |ORDER BY key, name
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name, settingKey, id))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToUserSettingFilter(resultArr.head)
    }
  }

  def retrieveFilterSettings()(implicit user: TableauxUser): Future[Seq[UserSettingFilter[_]]] = {
    val select =
      s"""
         |SELECT
         |  key,
         |  value,
         |  created_at,
         |  updated_at,
         |  name,
         |  id
         |FROM
         |  user_settings_filter
         |WHERE
         |  user_id = ?
         |ORDER BY key, name
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(user.name))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToUserSettingFilter)
    }
  }

  def upsertGlobalSetting(
      settingKey: String,
      settingValue: String
  )(implicit user: TableauxUser): Future[UserSettingGlobal[_]] = {
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

  def upsertTableSetting(
      settingKey: String,
      settingValue: String,
      tableId: Long
  )(implicit user: TableauxUser): Future[UserSettingTable[_]] = {
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

  def upsertFilterSetting(
      settingKey: String,
      settingValue: String,
      settingName: String
  )(implicit user: TableauxUser): Future[UserSettingFilter[_]] = {
    for {
      result <- connection.query(
        s"""
           |INSERT INTO user_settings_filter
           |  (key, value, user_id, name)
           |VALUES
           |  (?, ?, ?, ?)
           |RETURNING id
          """.stripMargin,
        Json.arr(settingKey, settingValue, user.name, settingName)
      )
      id = insertNotNull(result).head.get[Long](0)
      setting <- retrieveFilterSetting(settingKey, id)
    } yield {
      setting
    }
  }

  def deleteTableSetting(settingKey: String, tableId: Long): Future[Unit] = {
    val delete = s"DELETE FROM user_settings_table WHERE key = ? AND table_id = ?"

    for {
      result <- connection.query(delete, Json.arr(settingKey, tableId))
      _ <- Future(deleteNotNull(result))
    } yield ()
  }

  def deleteTableSettings(tableId: Long): Future[Unit] = {
    val delete = s"DELETE FROM user_settings_table WHERE table_id = ?"

    for {
      result <- connection.query(delete, Json.arr(tableId))
    } yield ()
  }

  def deleteFilterSetting(settingKey: String, id: Long): Future[Unit] = {
    val delete = s"DELETE FROM user_settings_filter WHERE key = ? AND id = ?"

    for {
      result <- connection.query(delete, Json.arr(settingKey, id))
      _ <- Future(deleteNotNull(result))
    } yield ()
  }
}
