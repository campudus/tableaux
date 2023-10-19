package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.KeycloakAuthHandler
import com.campudus.tableaux.router.auth.permission.RoleModel

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

import com.typesafe.scalalogging.LazyLogging

object RouterRegistry extends LazyLogging {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext
  ): Router = {

    val vertx: Vertx = tableauxConfig.vertx

    val isAuthorization: Boolean = !tableauxConfig.authConfig.isEmpty

    implicit val roleModel: RoleModel = RoleModel(tableauxConfig.rolePermissions, isAuthorization)

    val mainRouter: Router = Router.router(vertx)

    // if (isAuthorization) {
    //   // val clientOptions: OAuth2ClientOptions = OAuth2ClientOptions.fromJson(tableauxConfig.authConfig)
    //   val clientOptions: OAuth2ClientOptions =
    //     OAuth2ClientOptions().setSite("https://keycloak.winora.grud.de/auth/realms/datacenter").setClientID(
    //       "grud-backend-test"
    //     ).setClientSecret("bEwZGOJ3kx86BzrLcoWjGT8JmcOLkY3C")
    //       .setFlow(OAuth2FlowType.AUTH_CODE)
    //   // .setSite(System.getProperty("oauth2.issuer", "http://localhost:8080/auth/realms/vertx"))
    //   // .setClientID(System.getProperty("oauth2.client_id", "demo-client"))
    //   // .setClientSecret(System.getProperty("oauth2.client_secret", "ff160ac9-6989-4940-8013-883e81b39702"));
    //   val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)

    //   KeycloakAuth.discover(
    //     vertx,
    //     clientOptions,
    //     handler => {
    //       if (handler.succeeded()) {
    //         val keycloakAuthProvider = handler.result()
    //         val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
    //         // needed cookies will be extracted by route handlers and stored in `TableauxUser` instances
    //         mainRouter.route().handler(CookieHandler.create())
    //         mainRouter.route().handler(tableauxKeycloakAuthHandler)
    //         mainRouter.route().handler(keycloakAuthHandler)

    //         mainRouter.mountSubRouter("/system", systemRouter.route)
    //         mainRouter.mountSubRouter("/", structureRouter.route)
    //         mainRouter.mountSubRouter("/", tableauxRouter.route)
    //         mainRouter.mountSubRouter("/", mediaRouter.route)
    //         mainRouter.mountSubRouter("/docs", documentationRouter.route)

    //         mainRouter.get("/").handler(systemRouter.defaultRoute)
    //         mainRouter.get("/index.html").handler(systemRouter.defaultRoute)

    //         mainRouter.route().handler(systemRouter.noRouteMatched)
    //       } else {
    //         logger.error(
    //           "Could not configure Keycloak integration via OpenID Connect Discovery Endpoint. Is Keycloak running?"
    //         )
    //         logger.error(handler.cause().toString())
    //       }
    //     }
    //   )

    //   // val keycloakAuthProvider = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
    //   // val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
    //   // mainRouter.route().handler(keycloakAuthHandler)

    //   // mainRouter.route().handler(tableauxKeycloakAuthHandler)

    // } else {
    //   logger.warn(
    //     "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! " +
    //       "This is for development and/or testing purposes ONLY."
    //   )
    // }

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

    if (isAuthorization) {
      // val clientOptions: OAuth2ClientOptions = OAuth2ClientOptions.fromJson(tableauxConfig.authConfig)
      val clientOptions: OAuth2ClientOptions =
        OAuth2ClientOptions().setSite("https://keycloak.winora.grud.de/auth/realms/datacenter").setClientID(
          "grud-backend-test"
        ).setClientSecret("bEwZGOJ3kx86BzrLcoWjGT8JmcOLkY3C")
          .setFlow(OAuth2FlowType.AUTH_CODE)
      // .setSite(System.getProperty("oauth2.issuer", "http://localhost:8080/auth/realms/vertx"))
      // .setClientID(System.getProperty("oauth2.client_id", "demo-client"))
      // .setClientSecret(System.getProperty("oauth2.client_secret", "ff160ac9-6989-4940-8013-883e81b39702"));
      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)

      KeycloakAuth.discover(
        vertx,
        clientOptions,
        handler => {
          if (handler.succeeded()) {
            val keycloakAuthProvider = handler.result()
            val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
            // needed cookies will be extracted by route handlers and stored in `TableauxUser` instances
            mainRouter.route().handler(CookieHandler.create())
            mainRouter.route().handler(keycloakAuthHandler)
            mainRouter.route().handler(tableauxKeycloakAuthHandler)

            mainRouter.mountSubRouter("/system", systemRouter.route)
            mainRouter.mountSubRouter("/", structureRouter.route)
            mainRouter.mountSubRouter("/", tableauxRouter.route)
            mainRouter.mountSubRouter("/", mediaRouter.route)
            mainRouter.mountSubRouter("/docs", documentationRouter.route)

            mainRouter.get("/").handler(systemRouter.defaultRoute)
            mainRouter.get("/index.html").handler(systemRouter.defaultRoute)

            mainRouter.route().handler(systemRouter.noRouteMatched)
          } else {
            logger.error(
              "Could not configure Keycloak integration via OpenID Connect Discovery Endpoint. Is Keycloak running?"
            )
            logger.error(handler.cause().toString())
          }
        }
      )

      // val keycloakAuthProvider = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
      // val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
      // mainRouter.route().handler(keycloakAuthHandler)

      // mainRouter.route().handler(tableauxKeycloakAuthHandler)

    } else {
      logger.warn(
        "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! " +
          "This is for development and/or testing purposes ONLY."
      )
    }
    // mainRouter.mountSubRouter("/system", systemRouter.route)
    // mainRouter.mountSubRouter("/", structureRouter.route)
    // mainRouter.mountSubRouter("/", tableauxRouter.route)
    // mainRouter.mountSubRouter("/", mediaRouter.route)
    // mainRouter.mountSubRouter("/docs", documentationRouter.route)

    // mainRouter.get("/").handler(systemRouter.defaultRoute)
    // mainRouter.get("/index.html").handler(systemRouter.defaultRoute)

    // mainRouter.route().handler(systemRouter.noRouteMatched)

    mainRouter
  }
}
