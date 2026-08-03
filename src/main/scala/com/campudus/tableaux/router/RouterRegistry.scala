package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{
  MediaController,
  StructureController,
  SystemController,
  TableauxController,
  UserController
}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.KeycloakAuthHandler
import com.campudus.tableaux.router.auth.permission.RoleModel

import io.vertx.core.Vertx
import io.vertx.ext.auth.oauth2.OAuth2Options
import io.vertx.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.ext.web.{Router, RoutingContext}
import io.vertx.ext.web.handler.OAuth2AuthHandler
import io.vertx.lang.scala.VertxExecutionContext

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

    // Vert.x 4's OAuth2AuthHandler lets a raw RuntimeException from the underlying JWK signature check escape
    // uncaught (instead of properly failing the context with a 401 HttpException like it does for other token
    // validation problems, e.g. expiry/audience), which otherwise surfaces to clients as a bare 500. Recognize
    // that one known case and report it as an authentication failure instead of an internal server error.
    mainRouter.errorHandler(
      500,
      context => {
        val cause = context.failure()
        val looksLikeSignatureFailure =
          cause != null && Option(cause.getMessage).exists(_.toLowerCase.contains("signature"))

        if (!context.response().ended()) {
          context.response().setStatusCode(if (looksLikeSignatureFailure) 401 else 500).end()
        }
      }
    )

    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel, tableauxConfig)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection, fileModel)
    val serviceModel = ServiceModel(dbConnection)
    val cellAnnotationConfigModel = CellAnnotationConfigModel(dbConnection)
    val userModel = UserModel(dbConnection)

    val systemRouter =
      SystemRouter(
        tableauxConfig,
        SystemController(
          _,
          systemModel,
          tableauxModel,
          structureModel,
          serviceModel,
          roleModel,
          cellAnnotationConfigModel
        )
      )
    val tableauxRouter = TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel, roleModel))
    val mediaRouter =
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel, roleModel, tableauxModel))
    val structureRouter =
      StructureRouter(tableauxConfig, config => StructureController(config, structureModel, roleModel)())
    val documentationRouter = DocumentationRouter(tableauxConfig)
    val userRouter = UserRouter(tableauxConfig, UserController(_, userModel, roleModel))

    def registerCommonRoutes(router: Router) = {
      router.mountSubRouter("/system", systemRouter.route)
      router.mountSubRouter("/", structureRouter.route)
      router.mountSubRouter("/", tableauxRouter.route)
      router.mountSubRouter("/", mediaRouter.route)
      router.mountSubRouter("/docs", documentationRouter.route)
      router.mountSubRouter("/user", userRouter.route)

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
      val keycloakAuthHandler = OAuth2AuthHandler.create(vertx, keycloakAuthProvider)
      mainRouter.route().handler(keycloakAuthHandler)

      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)
      mainRouter.route().handler(tableauxKeycloakAuthHandler)

      registerCommonRoutes(mainRouter)
    }

    def initAutoDiscoverAuth() = {
      val clientOptions: OAuth2Options = new OAuth2Options()
        .setSite(tableauxConfig.authConfig.getString("issuer"))
        .setClientId(tableauxConfig.authConfig.getString("resource"))

      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)

      KeycloakAuth.discover(
        vertx,
        clientOptions,
        handler => {
          if (handler.succeeded()) {
            registerPublicRoutes(mainRouter)

            val keycloakAuthProvider = handler.result()
            val keycloakAuthHandler = OAuth2AuthHandler.create(vertx, keycloakAuthProvider)
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
