package com.campudus.tableaux.controller

import com.campudus.tableaux.{InvalidJsonException, TableauxConfig}
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.InvalidUserSettingException
import com.campudus.tableaux.NotFoundInDatabaseException
import com.campudus.tableaux.database.domain.{UserSetting, UserSettingKind, UserSettingSeq}
import com.campudus.tableaux.database.model.UserModel
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

  def retrieveSettings(kind: Option[String])(implicit user: TableauxUser): Future[UserSettingSeq] = {
    logger.info(s"retrieveSettings user: ${user.name}")

    for {
      settingsSeq <- repository.retrieveSettings(kind)
    } yield {
      UserSettingSeq(settingsSeq)
    }
  }

  def upsertSetting(settingKey: String, settingValue: String, tableId: Option[Long], name: Option[String])(implicit
  user: TableauxUser): Future[UserSetting] = {

    for {
      _ <- repository.checkSettingKey(settingKey)
      settingSchema <- repository.retrieveSettingSchema(settingKey)
      settingValidator = SchemaLoader.load(new JSONObject(settingSchema))
      _ <- Try(settingValidator.validate(new JSONObject(settingValue))) match {
        case Success(v) => Future.successful(())
        case Failure(e) => Future.failed(InvalidJsonException("setting value did not match schema", "invalid"))
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
}
