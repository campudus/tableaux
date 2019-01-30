package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.helper.JsonUtils.asCastedList
import com.campudus.tableaux.{InvalidNonceException, InvalidRequestException, NoNonceException, TableauxConfig}
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.concurrent.Future

object SystemRouter {
  private var nonce: Option[String] = None
  private var devMode: Boolean = sys.env.get("ENV").isDefined && sys.env("ENV") == "development"

  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => SystemController): SystemRouter = {
    new SystemRouter(config, controllerCurry(config))
  }

  def retrieveNonce(): Option[String] = nonce

  def generateNonce(): String = {
    val nonceString = UUID.randomUUID().toString
    nonce = Some(nonceString)
    nonceString
  }

  def invalidateNonce(): Unit = nonce = None

  def isDevMode: Boolean = devMode

  def setDevMode(boolean: Boolean): Boolean = {
    devMode = boolean
    devMode
  }
}

class SystemRouter(override val config: TableauxConfig, val controller: SystemController) extends BaseRouter {

  def getRouter(): Router = {
    val router = Router.router(vertx)

    router.get("/versions").handler(retrieveVersions)
    router.get("/settings/:settings").handler(retrieveSettings)

    router.post("/reset").handler(reset)
    router.post("/resetDemo").handler(resetDemo)
    router.post("/update").handler(update)
    router.post("/cache/invalidate").handler(invalidateCache)

    router.post("/settings/:settings").handler(updateSettings)

    router
      .postWithRegex("\\/cache\\/invalidate\\/tables\\/(?<tableId>[\\d^\\/]+)\\/columns\\/(?<columnId>[\\d^\\/]+)")
      .handler(invalidateColumnCache)

    router
  }

  /**
    * Get the current version
    */
  def retrieveVersions(context: RoutingContext): Unit = {
    sendReply(context.request(), asyncGetReply {
      controller.retrieveVersions()
    })
  }

  /**
    * Resets the database (needs nonce)
    */
  def reset(context: RoutingContext): Unit = {
    sendReply(context.request(), asyncGetReply {
      for {
        _ <- Future(checkNonce(context))
        result <- controller.resetDB()
      } yield result
    })
  }

  /**
    * Create the demo tables (needs nonce)
    */
  def resetDemo(context: RoutingContext): Unit = {
    sendReply(context.request(), asyncGetReply {
      for {
        _ <- Future(checkNonce(context))
        result <- controller.createDemoTables()
      } yield result
    })
  }

  /**
    * Update the database (needs POST and nonce)
    */
  def update(context: RoutingContext): Unit = {
    sendReply(context.request(), asyncGetReply {
      for {
        _ <- Future(checkNonce(context))
        result <- controller.updateDB()
      } yield result
    })
  }

  /**
    * Invalidate all caches
    */
  def invalidateCache(context: RoutingContext): Unit = {
    sendReply(context.request(), asyncGetReply {
      controller.invalidateCache()
    })
  }

  /**
    * Invalidate column cache
    */
  def invalidateColumnCache(context: RoutingContext): Unit = {
    val tableIdOpt: Option[TableId] = getLongParam("tableId", context)
    val columnIdOpt: Option[ColumnId] = getLongParam("columnId", context)

    (tableIdOpt, columnIdOpt) match {
      case (Some(tableId: Long), Some(columnId: Long)) =>
        sendReply(
          context.request(),
          asyncGetReply {
            controller.invalidateCache(tableId, columnId)
          }
        )
      case (_, _) => context.next()
    }
  }

  /**
    * Retrieve system settings
    */
  def retrieveSettings(context: RoutingContext): Unit = {
    val req = context.request()
    val settingOpt: Option[String] = req.getParam("settings")

    sendReply(
      req,
      asyncGetReply {
        settingOpt match {
          case Some(SystemController.SETTING_LANGTAGS) =>
            controller.retrieveLangtags()
          case Some(SystemController.SETTING_SENTRY_URL) =>
            controller.retrieveSentryUrl()
          case _ =>
            Future.failed(InvalidRequestException(s"No system setting for key $settingOpt"))
        }
      }
    )
  }

  /**
    * Update system settings
    */
  def updateSettings(context: RoutingContext): Unit = {
    val req = context.request()
    val settingOpt: Option[String] = req.getParam("settings")

    sendReply(
      req,
      asyncGetReply {
        getJson(context).flatMap(json => {
          settingOpt match {
            case Some(SystemController.SETTING_LANGTAGS) =>
              controller.updateLangtags(asCastedList[String](json.getJsonArray("value")).get)
            case Some(SystemController.SETTING_SENTRY_URL) =>
              controller.updateSentryUrl(json.getString("value"))
            case _ =>
              Future.failed(InvalidRequestException(s"No system setting for key $settingOpt"))
          }
        })
      }
    )
  }

  private def checkNonce(implicit context: RoutingContext): Unit = {
    val requestNonce = getStringParam("nonce", context)

    if (SystemRouter.isDevMode) {
      logger.warn(s"Seems like you are in development mode. Nonce is not checked.")
    } else if (SystemRouter.retrieveNonce().isEmpty) {
      SystemRouter.generateNonce()
      logger.warn(s"Generated a new nonce: ${SystemRouter.nonce}")
      throw NoNonceException("No nonce available. Generated new nonce.")
    } else if (requestNonce.isEmpty || requestNonce != SystemRouter.retrieveNonce()) {
      SystemRouter.generateNonce()
      logger.warn(s"Generated a new nonce: ${SystemRouter.nonce}")
      throw InvalidNonceException("Nonce can't be empty and must be valid. Generated new nonce.")
    } else {
      if (requestNonce == SystemRouter.retrieveNonce()) {
        logger.info("Nonce is correct and will be invalidated now.")
      }

      // in this case nonce and requestNonce must be the same
      // so nonce was used, let's invalidate it
      SystemRouter.invalidateNonce()
    }
  }
}
