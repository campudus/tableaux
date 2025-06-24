package com.campudus.tableaux.controller

import com.campudus.tableaux.{InvalidJsonException, TableauxConfig}
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.InvalidUserSettingException
import com.campudus.tableaux.NotFoundInDatabaseException
import com.campudus.tableaux.database.domain.{UserSetting, UserSettingKind, UserSettingSeq}
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
import com.campudus.tableaux.database.domain.EmptyObject

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

  def retrieveSettings(
      kind: Option[String]
  )(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveSettings user: ${user.name}, kind: $kind")

    for {
      settingsSeq <- repository.retrieveSettings(kind)
    } yield {
      UserSettingSeq(settingsSeq)
    }
  }

  def upsertSetting(
      settingKey: String,
      settingJson: JsonObject,
      tableId: Option[Long],
      name: Option[String]
  )(implicit user: TableauxUser): Future[UserSetting] = {
    logger.info(s"upsertSetting user: ${user.name}, key: $settingKey, tableId: $tableId, name: $name, json: $settingJson")

    for {
      _ <- repository.checkSettingKey(settingKey)
      settingSchema <- repository.retrieveSettingSchema(settingKey)

      settingValue <-
        if (!settingJson.containsKey("value"))
          Future.failed(InvalidJsonException("request must contain a value property", "value_prop_is_missing"))
        else if (settingJson.fieldNames().size() > 1)
          Future.failed(InvalidJsonException("request must only contain a value property", "value_prop_only"))
        else Future.successful(settingJson.getValue("value") match {
          case obj: JsonObject => obj.encode()
          case arr: JsonArray => arr.encode()
          case value => value.toString
        })

      settingValidatorLoader = SchemaLoader.builder().schemaJson(new JSONObject(settingSchema)).draftV7Support().build()
      settingValidator = settingValidatorLoader.load().build()
      // we need to use JSONObject as base for validation
      settingValidationValue = new JSONObject(settingJson.encode()).get("value")
      _ <- Try(settingValidator.validate(settingValidationValue)) match {
        case Success(v) => Future.successful(())
        case Failure(e) => {
          logger.error(s"error $e")
          Future.failed(InvalidJsonException("setting value did not match schema", "invalid"))
        }
      }
      settingKind <- repository.retrieveSettingKind(settingKey)
      setting <- settingKind match {
        case UserSettingKind.GLOBAL => {
          repository.upsertGlobalSetting(settingKey, settingValue)
        }
        case UserSettingKind.TABLE => {
          checkArguments(isDefined(tableId, "tableId"))
          repository.upsertTableSetting(settingKey, settingValue, tableId.get)
        }
        case UserSettingKind.FILTER => {
          checkArguments(isDefined(name, "name"))
          repository.upsertFilterSetting(settingKey, settingValue, name.get)
        }
        case _ => Future.failed(NotFoundInDatabaseException("setting not found", "user-setting"))
      }
    } yield {
      setting
    }
  }

  def deleteSetting(
      settingKey: String,
      tableId: Option[Long],
      id: Option[Long]
  )(implicit user: TableauxUser): Future[EmptyObject] = {
    logger.info(s"deleteSetting user: ${user.name}, key: $settingKey, tableId: $tableId, id: $id")

    for {
      _ <- repository.checkSettingKey(settingKey)
      settingKind <- repository.retrieveSettingKind(settingKey)
      setting <- settingKind match {
        case UserSettingKind.GLOBAL => {
          repository.deleteGlobalSetting(settingKey)
        }
        case UserSettingKind.TABLE => {
          checkArguments(isDefined(tableId, "tableId"))
          repository.deleteTableSetting(settingKey, tableId.get)
        }
        case UserSettingKind.FILTER => {
          checkArguments(isDefined(id, "id"))
          repository.deleteFilterSetting(settingKey, id.get)
        }
        case _ => Future.failed(NotFoundInDatabaseException("setting not found", "user-setting"))
      }
    } yield EmptyObject()
  }
}
