package com.campudus.tableaux.controller

import com.campudus.tableaux.{InvalidJsonException, TableauxConfig}
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.InvalidUserSettingException
import com.campudus.tableaux.NotFoundInDatabaseException
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.UserModel
import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}
import com.campudus.tableaux.verticles.EventClient

import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.Try

import org.everit.json.schema.Schema
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject

object UserController {

  def apply(
      config: TableauxConfig,
      repository: UserModel,
      roleModel: RoleModel
  ): UserController = {
    new UserController(
      config,
      repository,
      roleModel
    )
  }
}

class UserController(
    override val config: TableauxConfig,
    override protected val repository: UserModel,
    implicit protected val roleModel: RoleModel
) extends Controller[UserModel] {
  val eventClient: EventClient = EventClient(vertx)

  private def parseSettingJson[T](parseFn: => T): Future[T] = {
    Try(parseFn) match {
      case Success(value) if value != null => Future.successful(value)
      case _ => Future.failed(InvalidJsonException("json is invalid", "invalid"))
    }
  }

  def retrieveSettings(
      kind: Option[UserSettingKind]
  )(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveSettings user: ${user.name}, kind: $kind")

    for {
      settingsSeq <- kind match {
        case None => {
          for {
            globalSettings <- repository.retrieveGlobalSettings()
            tableSettings <- repository.retrieveTableSettings()
            filterSettings <- repository.retrieveFilterSettings()
          } yield {
            Seq(
              globalSettings.map(_.asInstanceOf[UserSetting[_]]),
              tableSettings.map(_.asInstanceOf[UserSetting[_]]),
              filterSettings.map(_.asInstanceOf[UserSetting[_]])
            ).flatten
          }
        }
        case Some(UserSettingKindGlobal) => repository.retrieveGlobalSettings()
        case Some(UserSettingKindTable) => repository.retrieveTableSettings()
        case Some(UserSettingKindFilter) => repository.retrieveFilterSettings()
      }
    } yield {
      UserSettingSeq(settingsSeq)
    }
  }

  def upsertGlobalSetting(
      settingKey: String,
      settingJson: JsonObject
  )(implicit user: TableauxUser): Future[UserSettingGlobal[_]] = {
    logger.info(s"upsertGlobalSetting user: ${user.name}, key: $settingKey, json: $settingJson")

    val settingKeyGlobal = UserSettingKeyGlobal.fromKey(settingKey)

    for {
      settingValue <- settingKeyGlobal match {
        case Some(UserSettingKeyGlobal.MarkdownEditor) =>
          parseSettingJson(settingJson.getJsonObject("value")).map(_.encode())
        case Some(UserSettingKeyGlobal.FilterReset) =>
          parseSettingJson(settingJson.getBoolean("value")).map(_.toString())
        case Some(UserSettingKeyGlobal.ColumnsReset) =>
          parseSettingJson(settingJson.getBoolean("value")).map(_.toString())
        case Some(UserSettingKeyGlobal.AnnotationReset) =>
          parseSettingJson(settingJson.getBoolean("value")).map(_.toString())
        case Some(UserSettingKeyGlobal.SortingReset) =>
          parseSettingJson(settingJson.getBoolean("value")).map(_.toString())
        case Some(UserSettingKeyGlobal.SortingDesc) =>
          parseSettingJson(settingJson.getBoolean("value")).map(_.toString())
        case None => Future.failed(InvalidUserSettingException("user setting key is invalid."))
      }
      setting <- repository.upsertGlobalSetting(settingKey, settingValue)
    } yield {
      setting
    }
  }

  def upsertTableSetting(
      settingKey: String,
      settingJson: JsonObject,
      tableId: Long
  )(implicit user: TableauxUser): Future[UserSettingTable[_]] = {
    logger.info(s"upsertTableSetting user: ${user.name}, key: $settingKey, json: $settingJson, tableId: $tableId")

    val settingKeyTable = UserSettingKeyTable.fromKey(settingKey)

    for {
      settingValue <- settingKeyTable match {
        case Some(UserSettingKeyTable.AnnotationHighlight) =>
          parseSettingJson(settingJson.getString("value"))
        case Some(UserSettingKeyTable.ColumnOrdering) =>
          parseSettingJson(settingJson.getJsonArray("value")).map(_.encode())
        case Some(UserSettingKeyTable.ColumnWidths) =>
          parseSettingJson(settingJson.getJsonObject("value")).map(_.encode())
        case Some(UserSettingKeyTable.RowsFilter) =>
          parseSettingJson(settingJson.getJsonObject("value")).map(_.encode())
        case Some(UserSettingKeyTable.VisibleColumns) =>
          parseSettingJson(settingJson.getJsonArray("value")).map(_.encode())
        case None => Future.failed(InvalidUserSettingException("user setting key is invalid."))
      }
      setting <- repository.upsertTableSetting(settingKey, settingValue, tableId)
    } yield {
      setting
    }
  }

  def upsertFilterSetting(
      settingKey: String,
      settingJson: JsonObject
  )(implicit user: TableauxUser): Future[UserSettingFilter[_]] = {
    logger.info(s"upsertFilterSetting user: ${user.name}, key: $settingKey, json: $settingJson")

    val settingKeyFilter = UserSettingKeyFilter.fromKey(settingKey)

    for {
      (settingValue, settingName) <- settingKeyFilter match {
        case Some(UserSettingKeyFilter.PresetFilter) => {
          for {
            value <- parseSettingJson(settingJson.getJsonObject("value")).map(_.encode())
            name <- parseSettingJson(settingJson.getString("name"))
          } yield (value, name)
        }
        case None => Future.failed(InvalidUserSettingException("user setting key is invalid."))
      }
      setting <- repository.upsertFilterSetting(settingKey, settingValue, settingName)
    } yield {
      setting
    }
  }

  def deleteTableSetting(
      settingKey: String,
      tableId: Long
  )(implicit user: TableauxUser): Future[EmptyObject] = {
    logger.info(s"deleteTableSetting user: ${user.name}, key: $settingKey, tableId: $tableId")

    val settingKeyTable = UserSettingKeyTable.fromKey(settingKey)

    for {
      _ <- settingKeyTable match {
        case Some(_) => repository.deleteTableSetting(settingKey, tableId)
        case None => Future.failed(InvalidUserSettingException("user setting key is invalid."))
      }
    } yield {
      EmptyObject()
    }
  }

  def deleteFilterSetting(
      settingKey: String,
      id: Long
  )(implicit user: TableauxUser): Future[EmptyObject] = {
    logger.info(s"deleteFilterSetting user: ${user.name}, key: $settingKey, id: $id")

    val settingKeyFilter = UserSettingKeyFilter.fromKey(settingKey)

    for {
      _ <- settingKeyFilter match {
        case Some(_) => repository.deleteFilterSetting(settingKey, id)
        case None => Future.failed(InvalidUserSettingException("user setting key is invalid."))
      }
    } yield {
      EmptyObject()
    }
  }
}
