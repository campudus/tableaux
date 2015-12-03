package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.{InvalidNonceException, NoNonceException, TableauxConfig}
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing.{Get, Post}

import scala.concurrent.Future
import scala.util.Try

object SystemRouter {
  var nonce: String = null

  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => SystemController): SystemRouter = {
    new SystemRouter(config, controllerCurry(config))
  }

  def retrieveNonce(): Option[String] = {
    Try(nonce).toOption.flatMap {
      Option(_)
    }
  }

  def generateNonce(): String = {
    nonce = UUID.randomUUID().toString
    nonce
  }
}

class SystemRouter(override val config: TableauxConfig, val controller: SystemController) extends BaseRouter {

  override def routes(implicit context: RoutingContext): Routing = {
    case Post("/reset") | Get("/reset") => asyncSetReply {
      for {
        _ <- Future(checkNonce)
        result <- controller.resetDB()
      } yield result
    }
    case Post("/resetDemo") | Get("/resetDemo") => asyncSetReply {
      for {
        _ <- Future(checkNonce)
        result <- controller.createDemoTables()
      } yield result
    }
    case Get("/system/versions") => asyncGetReply {
      controller.retrieveVersions()
    }
  }

  def checkNonce(implicit context: RoutingContext): Unit = {
    val requestNonce = getStringParam("nonce", context)

    if (SystemRouter.retrieveNonce().isEmpty) {
      SystemRouter.generateNonce()
      logger.info(s"Generated a new nonce: ${SystemRouter.nonce}")
      throw NoNonceException("No nonce available. Generated new nonce.")
    } else if (requestNonce.isEmpty || requestNonce.get != SystemRouter.retrieveNonce().get) {
      SystemRouter.generateNonce()
      logger.info(s"Generated a new nonce: ${SystemRouter.nonce}")
      throw InvalidNonceException("Nonce can't be empty and must be valid. Generated new nonce.")
    }

    SystemRouter.nonce = null
  }
}