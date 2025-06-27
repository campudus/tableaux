package com.campudus.tableaux.router

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

    router.putWithRegex(s"/settings/$settingKindGlobal/$settingKeyRegex").handler(upsertGlobalSetting)
    router.putWithRegex(s"/settings/$settingKindTable/$settingKeyRegex/$settingTableIdRegex").handler(
      upsertTableSetting
    )
    router.putWithRegex(s"/settings/$settingKindFilter/$settingKeyRegex").handler(upsertFilterSetting)

    router.deleteWithRegex(s"/settings/$settingKindTable/$settingTableIdRegex").handler(deleteTableSettings)
    router.deleteWithRegex(s"/settings/$settingKindTable/$settingKeyRegex/$settingTableIdRegex").handler(
      deleteTableSetting
    )
    router.deleteWithRegex(s"/settings/$settingKindFilter/$settingKeyRegex/$settingIdRegex").handler(
      deleteFilterSetting
    )

    router
  }

  private def getSettingKey(context: RoutingContext): Option[String] = {
    getStringParam("key", context)
  }

  private def getSettingTableId(context: RoutingContext): Option[Long] = {
    getLongParam("tableId", context)
  }

  private def getSettingId(context: RoutingContext): Option[Long] = {
    getLongParam("id", context)
  }

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

  private def upsertGlobalSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val settingJson = getJson(context)

          controller.upsertGlobalSetting(settingKey, settingJson)
        }
      )
    }
  }

  private def upsertTableSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
      settingTableId <- getSettingTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val settingJson = getJson(context)

          controller.upsertTableSetting(settingKey, settingJson, settingTableId)
        }
      )
    }
  }

  private def upsertFilterSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val settingJson = getJson(context)

          controller.upsertFilterSetting(settingKey, settingJson)
        }
      )
    }
  }

  private def deleteTableSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
      settingTableId <- getSettingTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteTableSetting(settingKey, settingTableId)
        }
      )
    }
  }

  private def deleteTableSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingTableId <- getSettingTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteTableSettings(settingTableId)
        }
      )
    }
  }

  private def deleteFilterSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
      settingId <- getSettingId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteFilterSetting(settingKey, settingId)
        }
      )
    }
  }
}
