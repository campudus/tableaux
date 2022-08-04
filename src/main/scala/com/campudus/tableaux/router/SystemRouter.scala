package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.database.domain.{MultiLanguageValue, ServiceType}
import com.campudus.tableaux.helper.JsonUtils.asCastedList
import com.campudus.tableaux.{InvalidNonceException, InvalidRequestException, NoNonceException, TableauxConfig}
import io.vertx.scala.ext.web.handler.BodyHandler
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.concurrent.Future
import scala.util.Try

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

  private val serviceId = """(?<serviceId>[\d]+)"""

  def route: Router = {
    val router = Router.router(vertx)

    router.get("/versions").handler(retrieveVersions)
    router.get("/settings/:settings").handler(retrieveSettings)
    router.get("/services").handler(retrieveServices)
    router.getWithRegex(s"""/services/$serviceId""").handler(retrieveService)

    router.deleteWithRegex(s"""/services/$serviceId""").handler(deleteService)

    router.post("/reset").handler(reset)
    router.post("/resetDemo").handler(resetDemo)
    router.post("/update").handler(update)
    router.post("/cache/invalidate").handler(invalidateCache)

    router.postWithRegex(s"/cache/invalidate/tables/$tableId/columns/$columnId").handler(invalidateColumnCache)

    // init body handler for settings routes
    router.post("/settings/*").handler(BodyHandler.create())

    router.post("/settings/:settings").handler(updateSettings)

    // init body handler for settings routes
    val bodyHandler = BodyHandler.create()
    router.post("/services").handler(bodyHandler)
    router.patch("/services/*").handler(bodyHandler)

    router.post("/services").handler(createService)
    router.patchWithRegex(s"""/services/$serviceId""").handler(updateService)

    router
  }

  /**
    * Get the current version
    */
  private def retrieveVersions(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    sendReply(
      context,
      asyncGetReply {
        controller.retrieveVersions()
      }
    )
  }

  /**
    * Resets the database (needs nonce)
    */
  private def reset(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    sendReply(
      context,
      asyncGetReply {
        for {
          _ <- Future.successful(checkNonce(context))
          result <- controller.resetDB()
        } yield result
      }
    )
  }

  /**
    * Create the demo tables (needs nonce)
    */
  private def resetDemo(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    sendReply(
      context,
      asyncGetReply {
        for {
          _ <- Future.successful(checkNonce(context))
          result <- controller.createDemoTables()
        } yield result
      }
    )
  }

  /**
    * Update the database (needs POST and nonce)
    */
  private def update(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    sendReply(
      context,
      asyncGetReply {
        for {
          _ <- Future.successful(checkNonce(context))
          result <- controller.updateDB()
        } yield result
      }
    )
  }

  /**
    * Invalidate all caches
    */
  private def invalidateCache(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        controller.invalidateCache()
      }
    )
  }

  /**
    * Invalidate column cache
    */
  private def invalidateColumnCache(context: RoutingContext): Unit = {
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
  private def retrieveSettings(context: RoutingContext): Unit = {
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
  private def updateSettings(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    for {
      key <- getStringParam("settings", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)

          key match {
            case SystemController.SETTING_LANGTAGS =>
              controller.updateLangtags(asCastedList[String](json.getJsonArray("value")).get)
            case SystemController.SETTING_SENTRY_URL =>
              controller.updateSentryUrl(json.getString("value"))
            case _ =>
              Future.failed(InvalidRequestException(s"No system setting for key $key"))
          }
        }
      )
    }
  }

  /**
    * Retrieve all services
    */
  private def retrieveServices(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    sendReply(
      context,
      asyncGetReply {
        controller.retrieveServices()
      }
    )
  }

  /**
    * Retrieve single service
    */
  private def retrieveService(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    for {
      serviceId <- getServiceId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveService(serviceId)
        }
      )
    }
  }

  /**
    * Create a new service
    */
  private def createService(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)

        // mandatory fields
        val name = json.getString("name")
        val serviceType = ServiceType(Option(json.getString("type")))

        // optional fields
        val ordering = Try(json.getInteger("ordering").longValue()).toOption
        val displayName = MultiLanguageValue[String](getNullableObject("displayName")(json))
        val description = MultiLanguageValue[String](getNullableObject("description")(json))
        val active = Try[Boolean](json.getBoolean("active")).getOrElse(false)
        val config = getNullableObject("config")(json)
        val scope = getNullableObject("scope")(json)

        controller.createService(name, serviceType, ordering, displayName, description, active, config, scope)
      }
    )
  }

  /**
    * Update a service
    */
  private def updateService(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    for {
      serviceId <- getServiceId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)

          // optional fields
          val name = Option(json.getString("name"))
          val serviceType = Option(json.getString("type")).map(t => ServiceType(Option(t)))
          val ordering = Try(json.getInteger("ordering").longValue()).toOption
          val displayName = getNullableObject("displayName")(json).map(MultiLanguageValue[String])
          val description = getNullableObject("description")(json).map(MultiLanguageValue[String])
          val active = Option(json.getBoolean("active")).flatMap(Try[Boolean](_).toOption)
          val config = getNullableObject("config")(json)
          val scope = getNullableObject("scope")(json)

          controller
            .updateService(serviceId, name, serviceType, ordering, displayName, description, active, config, scope)
        }
      )
    }
  }

  /**
    * Delete a service
    */
  private def deleteService(context: RoutingContext): Unit = {
    implicit val rc = implicitly(context)
    for {
      serviceId <- getServiceId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteService(serviceId)
        }
      )
    }
  }

  private def getServiceId(context: RoutingContext): Option[Long] = {
    getLongParam("serviceId", context)
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
