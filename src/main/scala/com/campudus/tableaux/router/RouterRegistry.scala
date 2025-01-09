package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.KeycloakAuthHandler
import com.campudus.tableaux.router.auth.permission.RoleModel

import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.oauth2.OAuth2ClientOptions
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.{Router, RoutingContext}
import io.vertx.scala.ext.web.handler.CookieHandler
import io.vertx.scala.ext.web.handler.OAuth2AuthHandler

import com.typesafe.scalalogging.LazyLogging

object RouterRegistry extends LazyLogging {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext
  ): Router = {

    val vertx: Vertx = tableauxConfig.vertx

    val isAuth: Boolean = !tableauxConfig.authConfig.isEmpty
    val isAutoDiscovery: Boolean = tableauxConfig.authConfig.getBoolean("isAutoDiscovery", false)

    implicit val roleModel: RoleModel = RoleModel(tableauxConfig.rolePermissions, isAuth)

    val mainRouter: Router = Router.router(vertx)

    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel, tableauxConfig)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection, fileModel)
    val serviceModel = ServiceModel(dbConnection)
    val cellAnnotationConfigModel = CellAnnotationConfigModel(dbConnection)

    val systemRouter =
      SystemRouter(
        tableauxConfig,
        SystemController(_, systemModel, tableauxModel, structureModel, serviceModel, roleModel, cellAnnotationConfigModel)
      )
    val tableauxRouter = TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel, roleModel))
    val mediaRouter =
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel, roleModel))
    val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel, roleModel))
    val documentationRouter = DocumentationRouter(tableauxConfig)

    mainRouter.route().handler(CookieHandler.create())

    def registerCommonRoutes(router: Router) = {
      router.mountSubRouter("/system", systemRouter.route)
      router.mountSubRouter("/", structureRouter.route)
      router.mountSubRouter("/", tableauxRouter.route)
      router.mountSubRouter("/", mediaRouter.route)
      router.mountSubRouter("/docs", documentationRouter.route)

      router.get("/").handler(systemRouter.defaultRoute)
      router.get("/index.html").handler(systemRouter.defaultRoute)

      router.route().handler(systemRouter.noRouteMatched)
    }

    def registerPublicRoutes(router: Router) = {
      logger.info("Registering public routes")
      router.mountSubRouter("/", mediaRouter.publicRoute)
    }

    def initManualAuth() = {
      val keycloakAuthProvider = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
      val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
      mainRouter.route().handler(keycloakAuthHandler)

      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)
      mainRouter.route().handler(tableauxKeycloakAuthHandler)

      registerCommonRoutes(mainRouter)
    }

    def initAutoDiscoverAuth() = {
      val clientOptions: OAuth2ClientOptions = OAuth2ClientOptions()
        .setSite(tableauxConfig.authConfig.getString("issuer"))
        .setClientID(tableauxConfig.authConfig.getString("resource"))

      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)

      KeycloakAuth.discover(
        vertx,
        clientOptions,
        handler => {
          if (handler.succeeded()) {
            registerPublicRoutes(mainRouter)

            val keycloakAuthProvider = handler.result()
            val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
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
    }

    if (isAuth) {
      if (isAutoDiscovery) {
        initAutoDiscoverAuth()
      } else {
        logger.info(
          "Started with manual auth configuration! To use auto discovery " +
            "set 'auth.isAutoDiscovery' to true in your config and remove all other " +
            "auth.* configuration options but `issuer` and `resource`."
        )
        initManualAuth()
      }

    } else {
      logger.warn(
        "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! " +
          "This is for development and/or testing purposes ONLY."
      )
      registerPublicRoutes(mainRouter)
      registerCommonRoutes(mainRouter)
    }

    mainRouter
  }
}
