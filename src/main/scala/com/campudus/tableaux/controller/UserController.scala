package com.campudus.tableaux.controller

import com.campudus.tableaux.{InvalidJsonException, TableauxConfig}
import com.campudus.tableaux.database.domain.{UserSettingGlobal, UserSettingGlobalSeq, UserSettingKind}
import com.campudus.tableaux.database.domain.UserSettingKindGlobal
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

  def retrieveGlobalSettings()(implicit user: TableauxUser): Future[UserSettingGlobalSeq] = {
    logger.info(s"retrieveGlobalSettings user: ${user.name}")

    for {
      globalSettingsSeq <- repository.retrieveGlobalSettings()
    } yield {
      UserSettingGlobalSeq(globalSettingsSeq)
    }
  }

  def updateGlobalSetting(settingKey: String, settingValue: String)(implicit
  user: TableauxUser): Future[UserSettingGlobal] = {
    logger.info(s"updateGlobalSetting user: ${user.name} setting: $settingKey")

    for {
      settingSchema <- repository.retrieveSettingSchema(settingKey)
      settingValidator = SchemaLoader.load(new JSONObject(settingSchema))
      _ <- Try(settingValidator.validate(new JSONObject(settingValue))) match {
        case Success(v) => Future.successful(())
        case Failure(e) => Future.failed(InvalidJsonException("setting value did not match schema", "value_is_invalid"))
      }
      _ <- repository.updateGlobalSetting(settingKey, settingValue)
      globalSetting <- repository.retrieveGlobalSetting(settingKey)
    } yield {
      globalSetting
    }
  }
}
