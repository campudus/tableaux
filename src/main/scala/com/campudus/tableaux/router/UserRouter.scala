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

  private val keyRegex = """(?<key>[a-zA-Z0-9_-]+)"""
  private val tableIdRegex = """(?<tableId>[\d]+)"""
  private val idRegex = """(?<id>[\d]+)"""

  private val kindGlobal = UserSettingKindGlobal.name
  private val kindTable = UserSettingKindTable.name
  private val kindFilter = UserSettingKindFilter.name

  private val eventClient: EventClient = EventClient(vertx)

  def route: Router = {
    val router = Router.router(vertx)
    val bodyHandler = BodyHandler.create()
    router.put("/settings/*").handler(bodyHandler)

    router.get("/settings").handler(retrieveSettings)

    // global
    router.get(s"/settings/$kindGlobal").handler(retrieveGlobalSettings)
    router.putWithRegex(s"/settings/$kindGlobal/$keyRegex").handler(upsertGlobalSetting)

    // table
    router.get(s"/settings/$kindTable").handler(retrieveTableSettings)
    router.getWithRegex(s"/settings/$kindTable/$tableIdRegex").handler(retrieveTableSettings)
    router.putWithRegex(s"/settings/$kindTable/$tableIdRegex/$keyRegex").handler(upsertTableSetting)
    router.deleteWithRegex(s"/settings/$kindTable/$tableIdRegex").handler(deleteTableSettings)
    router.deleteWithRegex(s"/settings/$kindTable/$tableIdRegex/$keyRegex").handler(deleteTableSetting)

    // filter
    router.get(s"/settings/$kindFilter").handler(retrieveFilterSettings)
    router.putWithRegex(s"/settings/$kindFilter/$keyRegex").handler(upsertFilterSetting)
    router.deleteWithRegex(s"/settings/$kindFilter/$idRegex").handler(deleteFilterSetting)

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
    sendReply(context, asyncGetReply(controller.retrieveSettings()))
  }

  private def retrieveGlobalSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(context, asyncGetReply(controller.retrieveGlobalSettings()))
  }

  private def retrieveTableSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    val settingTableId = getSettingTableId(context)
    sendReply(context, asyncGetReply(controller.retrieveTableSettings(settingTableId)))
  }

  private def retrieveFilterSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(context, asyncGetReply(controller.retrieveFilterSettings()))
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
      settingId <- getSettingId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteFilterSetting(settingId)
        }
      )
    }
  }
}
