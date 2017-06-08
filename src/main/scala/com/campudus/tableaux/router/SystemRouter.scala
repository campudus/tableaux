package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.helper.JsonUtils.asCastedList
import com.campudus.tableaux.{InvalidNonceException, InvalidRequestException, NoNonceException, TableauxConfig}
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing.{Get, Post}

import scala.concurrent.Future
import scala.util.matching.Regex

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

  private val CacheColumnInvalidate: Regex = "/system/cache/invalidate/tables/(\\d+)/columns/(\\d+)".r
  private val Settings: Regex = "/system/settings/(\\w+)".r

  override def routes(implicit context: RoutingContext): Routing = {

    /**
      * Resets the database (needs nonce)
      */
    case Post("/system/reset") =>
      asyncGetReply {
        for {
          _ <- Future(checkNonce)
          result <- controller.resetDB()
        } yield result
      }

    /**
      * Create the demo tables (needs nonce)
      */
    case Post("/system/resetDemo") =>
      asyncGetReply {
        for {
          _ <- Future(checkNonce)
          result <- controller.createDemoTables()
        } yield result
      }

    /**
      * Get the current version
      */
    case Get("/system/versions") =>
      asyncGetReply {
        controller.retrieveVersions()
      }

    /**
      * Update the database (needs POST and nonce)
      */
    case Post("/system/update") =>
      asyncGetReply {
        for {
          _ <- Future(checkNonce)
          result <- controller.updateDB()
        } yield result
      }

    /**
      * Invalidate all caches
      */
    case Post("/system/cache/invalidate") =>
      asyncGetReply {
        controller.invalidateCache()
      }

    /**
      * Invalidate column cache
      */
    case Post(CacheColumnInvalidate(tableId, columnId)) =>
      asyncGetReply {
        controller.invalidateCache(tableId.toLong, tableId.toLong)
      }

    /**
      * Retrieves system settings
      */
    case Get(Settings(key)) =>
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

    /**
      * Update system settings
      */
    case Post(Settings(key)) =>
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
