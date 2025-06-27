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

  private def extractFromJson[T](extractFn: => T): Future[T] = {
    Try(extractFn) match {
      case Success(value) if value != null => Future.successful(value)
      case Success(value) if value == null => Future.failed(InvalidJsonException("value is null", "invalid"))
      case Failure(exception) => Future.failed(InvalidJsonException(exception.getMessage(), "invalid"))
    }
  }

  private def encode(value: Any): String = {
    value match {
      case s: String => "\"" + s.replace("\"", "\\\"") + "\""
      case b: Boolean => b.toString()
      case n: Int => n.toString()
      case n: Long => n.toString()
      case n: Double => n.toString()
      case n: Float => n.toString()
      case jo: JsonObject => jo.encode()
      case ja: JsonArray => ja.encode()
      case other => "\"" + other.toString().replace("\"", "\\\"") + "\""
    }
  }

  def retrieveSettings()(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveSettings user: ${user.name}")

    for {
      globalSettings <- repository.retrieveGlobalSettings()
      tableSettings <- repository.retrieveTableSettings()
      filterSettings <- repository.retrieveFilterSettings()
    } yield {
      UserSettingSeq(Seq(
        globalSettings.map(_.asInstanceOf[UserSetting[_]]),
        tableSettings.map(_.asInstanceOf[UserSetting[_]]),
        filterSettings.map(_.asInstanceOf[UserSetting[_]])
      ).flatten)
    }
  }

  def retrieveGlobalSettings()(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveGlobalSettings user: ${user.name}")

    for {
      globalSettings <- repository.retrieveGlobalSettings()
    } yield {
      UserSettingSeq(globalSettings)
    }
  }

  def retrieveTableSettings(
      tableId: Option[Long] = None
  )(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveTableSettings user: ${user.name}, tableId: $tableId")

    for {
      tableSettings <- repository.retrieveTableSettings(tableId)
    } yield {
      UserSettingSeq(tableSettings)
    }
  }

  def retrieveFilterSettings()(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveFilterSettings user: ${user.name}")

    for {
      filterSettings <- repository.retrieveFilterSettings()
    } yield {
      UserSettingSeq(filterSettings)
    }
  }

  def upsertGlobalSetting(
      settingKey: String,
      settingJson: JsonObject
  )(implicit user: TableauxUser): Future[UserSettingGlobal[_]] = {
    checkArguments(
      notNull(settingKey, "key"),
      notNull(settingJson, "json")
    )

    logger.info(s"upsertGlobalSetting user: ${user.name}, key: $settingKey, json: $settingJson")

    val settingKeyGlobal = UserSettingKeyGlobal.fromKey(settingKey)

    for {
      settingValue <- settingKeyGlobal match {
        case Some(UserSettingKeyGlobal.MarkdownEditor) =>
          extractFromJson(settingJson.getString("value")).map(encode(_))
        case Some(UserSettingKeyGlobal.FilterReset) =>
          extractFromJson(settingJson.getBoolean("value")).map(encode(_))
        case Some(UserSettingKeyGlobal.ColumnsReset) =>
          extractFromJson(settingJson.getBoolean("value")).map(encode(_))
        case Some(UserSettingKeyGlobal.AnnotationReset) =>
          extractFromJson(settingJson.getBoolean("value")).map(encode(_))
        case Some(UserSettingKeyGlobal.SortingReset) =>
          extractFromJson(settingJson.getBoolean("value")).map(encode(_))
        case Some(UserSettingKeyGlobal.SortingDesc) =>
          extractFromJson(settingJson.getBoolean("value")).map(encode(_))
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
    checkArguments(
      notNull(settingKey, "key"),
      notNull(settingJson, "json"),
      greaterZero(tableId)
    )

    logger.info(s"upsertTableSetting user: ${user.name}, key: $settingKey, json: $settingJson, tableId: $tableId")

    val settingKeyTable = UserSettingKeyTable.fromKey(settingKey)

    for {
      settingValue <- settingKeyTable match {
        case Some(UserSettingKeyTable.AnnotationHighlight) =>
          extractFromJson(settingJson.getString("value")).map(encode(_))
        case Some(UserSettingKeyTable.ColumnOrdering) =>
          extractFromJson(settingJson.getJsonArray("value")).map(encode(_))
        case Some(UserSettingKeyTable.ColumnWidths) =>
          extractFromJson(settingJson.getJsonObject("value")).map(encode(_))
        case Some(UserSettingKeyTable.RowsFilter) =>
          extractFromJson(settingJson.getJsonObject("value")).map(encode(_))
        case Some(UserSettingKeyTable.VisibleColumns) =>
          extractFromJson(settingJson.getJsonArray("value")).map(encode(_))
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
    checkArguments(
      notNull(settingKey, "key"),
      notNull(settingJson, "json")
    )

    logger.info(s"upsertFilterSetting user: ${user.name}, key: $settingKey, json: $settingJson")

    val settingKeyFilter = UserSettingKeyFilter.fromKey(settingKey)

    for {
      (settingValue, settingName) <- settingKeyFilter match {
        case Some(UserSettingKeyFilter.PresetFilter) => {
          for {
            value <- extractFromJson(settingJson.getJsonObject("value")).map(encode(_))
            // no encoding needed because it ends up in separate column of type varchar
            name <- extractFromJson(settingJson.getString("name"))
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
    checkArguments(
      notNull(settingKey, "key"),
      greaterZero(tableId)
    )

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

  def deleteTableSettings(tableId: Long)(implicit user: TableauxUser): Future[EmptyObject] = {
    checkArguments(
      greaterZero(tableId)
    )

    logger.info(s"deleteTableSettings user: ${user.name}, tableId: $tableId")

    for {
      _ <- repository.deleteTableSettings(tableId)
    } yield {
      EmptyObject()
    }
  }

  def deleteFilterSetting(
      settingKey: String,
      id: Long
  )(implicit user: TableauxUser): Future[EmptyObject] = {
    checkArguments(
      notNull(settingKey, "key"),
      greaterZero(id)
    )

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
