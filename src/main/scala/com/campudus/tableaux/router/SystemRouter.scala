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

  def retrieveNonce(): Option[String] = Option(nonce)

  def generateNonce(): String = {
    nonce = UUID.randomUUID().toString
    nonce
  }
}

class SystemRouter(override val config: TableauxConfig, val controller: SystemController) extends BaseRouter {

  override def routes(implicit context: RoutingContext): Routing = {

    /**
      * Resets the database (needs nonce)
      */
    case Post("/reset") => asyncGetReply {
      for {
        _ <- Future(checkNonce)
        result <- controller.resetDB()
      } yield result
    }

    /**
      * Create the demo tables (needs nonce)
      */
    case Post("/resetDemo") => asyncGetReply {
      for {
        _ <- Future(checkNonce)
        result <- controller.createDemoTables()
      } yield result
    }

    /**
      * Get the current version
      */
    case Get("/system/versions") => asyncGetReply {
      controller.retrieveVersions()
    }

    /**
      * Update the database (needs POST and nonce)
      */
    case Post("/system/update") => asyncGetReply {
      for {
        _ <- Future(checkNonce)
        result <- controller.updateDB()
      } yield result
    }
  }

  private def checkNonce(implicit context: RoutingContext): Unit = {
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