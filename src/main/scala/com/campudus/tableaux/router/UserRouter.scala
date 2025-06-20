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

  private val settingKey = """(?<settingKey>[a-zA-Z0-9_-]+)"""
  private val eventClient: EventClient = EventClient(vertx)

  def route: Router = {
    val router = Router.router(vertx)
    val bodyHandler = BodyHandler.create()
    router.patch("/settings/*").handler(bodyHandler)

    router.get("/settings/global").handler(retrieveGlobalSettings)
    router.patchWithRegex(s"/settings/global/$settingKey").handler(updateGlobalSetting)

    router
  }

  private def getSettingKey(context: RoutingContext): Option[String] = {
    getStringParam("settingKey", context)
  }

  private def retrieveGlobalSettings(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    sendReply(
      context,
      asyncGetReply {
        controller.retrieveGlobalSettings()
      }
    )
  }

  private def updateGlobalSetting(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)

    logger.info(s"updateGlobalSetting user: ${user.name}")

    for {
      settingKey <- getSettingKey(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val settingJson = getJson(context)

          if (!settingJson.containsKey("value")) {
            Future.failed(InvalidJsonException("request must contain a value property", "value_prop_is_missing"))
          } else if (settingJson.fieldNames().size() > 1) {
            Future.failed(InvalidJsonException("request must only contain a value property", "value_prop_only"))
          } else {
            controller.updateGlobalSetting(settingKey, settingJson.encode())
          }
        }
      )
    }
  }
}
