package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.KeycloakAuthHandler
import com.campudus.tableaux.router.auth.permission.RoleModel

import io.vertx.core.AsyncResult
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.ext.auth.oauth2.impl.OAuth2AuthProviderImpl
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.oauth2.OAuth2Auth
import io.vertx.scala.ext.auth.oauth2.OAuth2ClientOptions
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.{Router, RoutingContext}
import io.vertx.scala.ext.web.handler.CookieHandler
import io.vertx.scala.ext.web.handler.OAuth2AuthHandler

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging

object RouterRegistry extends LazyLogging {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext
  ): Router = {

    val vertx: Vertx = tableauxConfig.vertx

    val isAuthorization: Boolean = !tableauxConfig.authConfig.isEmpty

    implicit val roleModel: RoleModel = RoleModel(tableauxConfig.rolePermissions, isAuthorization)

    val mainRouter: Router = Router.router(vertx)

    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection, fileModel)
    val serviceModel = ServiceModel(dbConnection)

    val systemRouter =
      SystemRouter(
        tableauxConfig,
        SystemController(_, systemModel, tableauxModel, structureModel, serviceModel, roleModel)
      )
    val tableauxRouter = TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel, roleModel))
    val mediaRouter =
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel, roleModel))
    val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel, roleModel))
    val documentationRouter = DocumentationRouter(tableauxConfig)

    mainRouter.route().handler(CookieHandler.create())

    def registerCommonRoutes(router: Router) = {
      logger.info("### Registering routes...")
      router.mountSubRouter("/system", systemRouter.route)
      router.mountSubRouter("/", structureRouter.route)
      router.mountSubRouter("/", tableauxRouter.route)
      router.mountSubRouter("/", mediaRouter.route)
      router.mountSubRouter("/docs", documentationRouter.route)

      router.get("/").handler(systemRouter.defaultRoute)
      router.get("/index.html").handler(systemRouter.defaultRoute)

      router.route().handler(systemRouter.noRouteMatched)

      logger.info("### Routes registered.")
    }

    if (isAuthorization) {
      val clientOptions: OAuth2ClientOptions =
        OAuth2ClientOptions().setSite("https://keycloak.winora.grud.de/auth/realms/datacenter")
          .setClientID("grud-backend-test")
      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)

      logger.info("### Discovered KeycloakAuthHandler")

      KeycloakAuth.discover(
        vertx,
        clientOptions,
        handler => {
          logger.info("### Discovered KeycloakAuthHandler")
          if (handler.succeeded()) {
            logger.info("### Discovered KeycloakAuthHandler succeeded")
            // kHandlerP = handler.result()
            val keycloakAuthProvider = handler.result()
            val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
            // needed cookies will be extracted by route handlers and stored in `TableauxUser` instances
            mainRouter.route().handler(keycloakAuthHandler)
            mainRouter.route().handler(tableauxKeycloakAuthHandler)

            registerCommonRoutes(mainRouter)
          } else {
            logger.error(
              "Could not configure Keycloak integration via OpenID Connect " +
                "Discovery Endpoint because of: " + handler.cause().getMessage
            )
          }
        }
      )

    } else {
      logger.warn(
        "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! " +
          "This is for development and/or testing purposes ONLY."
      )
      logger.info("### Registering routes without auth...")
      registerCommonRoutes(mainRouter)
    }

    logger.info("### Routes registered!!!")
    mainRouter
  }
}
