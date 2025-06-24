package com.campudus.tableaux.router

import com.campudus.tableaux.InvalidJsonException
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.UserController
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

  private val settingKey = """(?<key>[a-zA-Z0-9_-]+)"""
  private val eventClient: EventClient = EventClient(vertx)

  def route: Router = {
    val router = Router.router(vertx)
    val bodyHandler = BodyHandler.create()
    router.put("/settings/*").handler(bodyHandler)

    router.get("/settings").handler(retrieveSettings)
    router.putWithRegex(s"/settings/$settingKey").handler(upsertSetting)
    router.deleteWithRegex(s"/settings/$settingKey").handler(deleteSetting)

    router
  }

  private def getSettingKey(context: RoutingContext): Option[String] = {
    getStringParam("key", context)
  }

  private def getSettingTableId(context: RoutingContext): Option[Long] = {
    getLongQuery("tableId", context)
  }

  private def getSettingName(context: RoutingContext): Option[String] = {
    getStringQuery("name", context)
  }

  private def getSettingId(context: RoutingContext): Option[Long] = {
    getLongQuery("id", context)
  }

  private def getSettingKind(context: RoutingContext): Option[String] = {
    getStringQuery("kind", context)
  }

  private def retrieveSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    sendReply(
      context,
      asyncGetReply {
        val settingKind = getSettingKind(context);
        controller.retrieveSettings(settingKind)
      }
    )

  }

  private def upsertSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val settingJson = getJson(context)
          val settingTableId = getSettingTableId(context)
          val settingName = getSettingName(context)

          controller.upsertSetting(settingKey, settingJson, settingTableId, settingName)
        }
      )
    }
  }

  private def deleteSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    for {
      settingKey <- getSettingKey(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val settingTableId = getSettingTableId(context)
          val settingId = getSettingId(context)

          controller.deleteSetting(settingKey, settingTableId, settingId)
        }
      )
    }
  }
}
