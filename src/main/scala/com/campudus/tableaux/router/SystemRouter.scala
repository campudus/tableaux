package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.helper.JsonUtils.asCastedList
import com.campudus.tableaux.{InvalidNonceException, InvalidRequestException, NoNonceException, TableauxConfig}
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.concurrent.Future

object SystemRouter {
  private var nonce: Option[String] = None
  private var devMode: Boolean = sys.env.get("ENV").isDefined && sys.env("ENV") == "development"

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => SystemController): SystemRouter = {
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

  def route: Router = {
    val router = Router.router(vertx)

    router.get("/versions").handler(retrieveVersions)
    router.get("/settings/:settings").handler(retrieveSettings)

    router.post("/reset").handler(reset)
    router.post("/resetDemo").handler(resetDemo)
    router.post("/update").handler(update)
    router.post("/cache/invalidate").handler(invalidateCache)

    router.post("/settings/:settings").handler(updateSettings)
    router.postWithRegex(s"""/cache/invalidate/tables/$TABLE_ID/columns/$COLUMN_ID""").handler(invalidateColumnCache)

    router
  }

  /**
    * Get the current version
    */
  def retrieveVersions(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      controller.retrieveVersions()
    })
  }

  /**
    * Resets the database (needs nonce)
    */
  def reset(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      for {
        _ <- Future.successful(checkNonce(context))
        result <- controller.resetDB()
      } yield result
    })
  }

  /**
    * Create the demo tables (needs nonce)
    */
  def resetDemo(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      for {
        _ <- Future.successful(checkNonce(context))
        result <- controller.createDemoTables()
      } yield result
    })
  }

  /**
    * Update the database (needs POST and nonce)
    */
  def update(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      for {
        _ <- Future.successful(checkNonce(context))
        result <- controller.updateDB()
      } yield result
    })
  }

  /**
    * Invalidate all caches
    */
  def invalidateCache(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      controller.invalidateCache()
    })
  }

  /**
    * Invalidate column cache
    */
  def invalidateColumnCache(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.invalidateCache(tableId, columnId)
        }
      )
    }
  }

  /**
    * Retrieve system settings
    */
  def retrieveSettings(context: RoutingContext): Unit = {
    for {
      key <- getStringParam("settings", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          key match {
            case SystemController.SETTING_LANGTAGS =>
              controller.retrieveLangtags()
            case SystemController.SETTING_SENTRY_URL =>
              controller.retrieveSentryUrl()
            case _ =>
              Future.failed(InvalidRequestException(s"No system setting for key $key"))
          }
        }
      )
    }
  }

  /**
    * Update system settings
    */
  def updateSettings(context: RoutingContext): Unit = {
    for {
      key <- getStringParam("settings", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          getJson(context).flatMap(json => {
            key match {
              case SystemController.SETTING_LANGTAGS =>
                controller.updateLangtags(asCastedList[String](json.getJsonArray("value")).get)
              case SystemController.SETTING_SENTRY_URL =>
                controller.updateSentryUrl(json.getString("value"))
              case _ =>
                Future.failed(InvalidRequestException(s"No system setting for key $key"))
            }
          })
        }
      )
    }
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
