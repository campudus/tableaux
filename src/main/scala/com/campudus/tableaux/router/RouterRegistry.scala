package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.{KeycloakAuthHandler, RoleModel}
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import com.typesafe.scalalogging.LazyLogging
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.OAuth2AuthHandler

object RouterRegistry extends LazyLogging {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext): Router = {

    val vertx = tableauxConfig.vertx

    val roleModel = RoleModel(tableauxConfig.rolePermissions)

    val mainRouter: Router = Router.router(vertx)

    implicit val requestContext: RequestContext = RequestContext()

    if (!tableauxConfig.authConfig.isEmpty) {
      val keycloakAuthProvider = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
      val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
      mainRouter.route().handler(keycloakAuthHandler)

      val tableauxKeycloakAuthHandler = new KeycloakAuthHandler(vertx, tableauxConfig)
      mainRouter.route().handler(tableauxKeycloakAuthHandler)

    } else {
      logger.warn(
        "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! " +
          "This is for development and/or testing purposes ONLY.")
    }

    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection, fileModel)
    val serviceModel = ServiceModel(dbConnection)

    val systemRouter =
      SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel, structureModel, serviceModel))
    val tableauxRouter = TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel))
    val mediaRouter =
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel, roleModel))
    val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel, roleModel))
    val documentationRouter = DocumentationRouter(tableauxConfig)

    mainRouter.mountSubRouter("/system", systemRouter.route)
    mainRouter.mountSubRouter("/", structureRouter.route)
    mainRouter.mountSubRouter("/", tableauxRouter.route)
    mainRouter.mountSubRouter("/", mediaRouter.route)
    mainRouter.mountSubRouter("/docs", documentationRouter.route)

    mainRouter.get("/").handler(systemRouter.defaultRoute)
    mainRouter.get("/index.html").handler(systemRouter.defaultRoute)

    mainRouter.route().handler(systemRouter.noRouteMatched)

    mainRouter
  }
}
