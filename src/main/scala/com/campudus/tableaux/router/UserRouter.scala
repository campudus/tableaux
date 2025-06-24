package com.campudus.tableaux.router

import com.campudus.tableaux.InvalidJsonException
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.UserController
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.router.auth.permission.TableauxUser
import com.campudus.tableaux.verticles.EventClient

import io.vertx.scala.ext.web.{Router, RoutingContext}
import io.vertx.scala.ext.web.handler.BodyHandler

import scala.concurrent.Future
import scala.util.Try

import java.util.UUID

object UserRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => UserController): UserRouter = {
    new UserRouter(config, controllerCurry(config))
  }
}

class UserRouter(override val config: TableauxConfig, val controller: UserController) extends BaseRouter {

  private val settingKeyRegex = """(?<key>[a-zA-Z0-9_-]+)"""
  private val settingTableIdRegex = """(?<tableId>[\d]+)"""
  private val settingIdRegex = """(?<id>[\d]+)"""
  private val settingNameRegex = """(?<name>[a-zA-Z0-9_-+]+)"""

  private val settingKindGlobal = UserSettingKindGlobal.name
  private val settingKindTable = UserSettingKindTable.name
  private val settingKindFilter = UserSettingKindFilter.name

  private val eventClient: EventClient = EventClient(vertx)

  def route: Router = {
    val router = Router.router(vertx)
    val bodyHandler = BodyHandler.create()
    router.put("/settings/*").handler(bodyHandler)

    router.get("/settings").handler(retrieveSettings)
    router.get(s"/settings/$settingKindGlobal").handler(retrieveGlobalSettings)
    router.get(s"/settings/$settingKindTable").handler(retrieveTableSettings)
    router.get(s"/settings/$settingKindFilter").handler(retrieveFilterSettings)

    // router.putWithRegex(s"/settings/$settingKindGlobal/$settingKeyRegex").handler(upsertGlobalSetting)
    // router.putWithRegex(s"/settings/$settingKindTable/$settingKeyRegex/$settingTableIdRegex").handler(upsertTableSetting)
    // router.putWithRegex(s"/settings/$settingKindFilter/$settingKeyRegex/$settingNameRegex").handler(upsertFilterSetting)

    // router.deleteWithRegex(s"/settings/$settingKindTable/$settingKeyRegex/$settingTableIdRegex").handler(deleteTableSetting)
    // router.deleteWithRegex(s"/settings/$settingKindFilter/$settingKeyRegex/$settingIdRegex").handler(deleteFilterSetting)

    router
  }

  private def getSettingKind(context: RoutingContext): Option[UserSettingKind] = {
    getStringParam("kind", context).map(UserSettingKind(_))
  }

  private def getSettingKey(context: RoutingContext): Option[String] = {
    getStringParam("key", context)
  }

  // private def getSettingTableId(context: RoutingContext): Option[Long] = {
  //   getLongQuery("tableId", context)
  // }

  // private def getSettingName(context: RoutingContext): Option[String] = {
  //   getStringQuery("name", context)
  // }

  // private def getSettingId(context: RoutingContext): Option[Long] = {
  //   getLongQuery("id", context)
  // }

  private def retrieveSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(context, asyncGetReply(controller.retrieveSettings(None)))
  }

  private def retrieveGlobalSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(context, asyncGetReply(controller.retrieveSettings(Some(UserSettingKindGlobal))))
  }

  private def retrieveTableSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(context, asyncGetReply(controller.retrieveSettings(Some(UserSettingKindTable))))
  }

  private def retrieveFilterSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(context, asyncGetReply(controller.retrieveSettings(Some(UserSettingKindFilter))))
  }

  // private def upsertGlobalSetting(context: RoutingContext): Unit = {
  //   implicit val user = TableauxUser(context)

  //   for {
  //     settingKey <- getSettingKey(context)
  //   } yield {
  //     sendReply(
  //       context,
  //       asyncGetReply {
  //         val settingJson = getJson(context)

  //         controller.upsertGlobalSetting(settingKey, settingJson)
  //       }
  //     )
  //   }
  // }

  // private def deleteSetting(context: RoutingContext): Unit = {
  //   implicit val user = TableauxUser(context)

  //   for {
  //     settingKey <- getSettingKey(context)
  //   } yield {
  //     sendReply(
  //       context,
  //       asyncGetReply {
  //         val settingTableId = getSettingTableId(context)
  //         val settingId = getSettingId(context)

  //         controller.deleteSetting(settingKey, settingTableId, settingId)
  //       }
  //     )
  //   }
  // }
}
