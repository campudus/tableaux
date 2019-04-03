package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.handler.{CookieHandler, OAuth2AuthHandler, UserSessionHandler}
import io.vertx.scala.ext.web.{Router, RoutingContext}
import org.vertx.scala.core.json.Json

import scala.util.{Failure, Success}

object RouterRegistry {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection): Router = {

    val vertx = tableauxConfig.vertx

    val mainRouter: Router = Router.router(vertx)

    implicit val requestContext: RequestContext = RequestContext()

//    // you would get this config from the keycloak admin console
    val keycloakJson = Json.fromObjectString(
      """
        |{
        |  "realm": "master",
        |  "realm-public-key": "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuOl1gQ0wcRTu9p/bysfzE57sKZt9LanodDih+sYVwB2eN7P6l+kZc+jALulKLNd8xmcCtUVNoE+wr10q+i99/RyJ7A2xDTCFtPuGL1wK2B2vQ/7r9n//7FRnf1CrVTg1QQPPFS5BypbojD7bJYWEvrVHo8WMHp9f/66M/cUQpSEIsnl0H7a8Lhv4snmXTbyJ0OJ2o5nKQhOrcguMtpTfnqMgSOd14UVKt8281sk0RkJFste4wzk8kujf6+AtOKlsQHCgG0nv/gBRmJr7aZscUOqUXsb4MDiBQ2AZyyciSKRFtXLdgELjQPmEc7mG3HIcuH73agen6yl5NgSdeJ7joQIDAQAB",
        |  "auth-server-url": "http://localhost:9999/auth",
        |  "ssl-required": "external",
        |  "resource": "grud",
        |  "credentials": {
        |    "secret": "e8b2f1ab-ea56-4276-864d-16180a2f03b9"
        |  },
        |  "confidential-port": 0,
        |  "public-client": true,
        |  "bearer-only": true,
        |  "always-refresh-token": false
        |}
      """.stripMargin
    )

    // Initialize the OAuth2 Library
    var oauth2 = KeycloakAuth.create(vertx, OAuth2FlowType.AUTH_CODE, keycloakJson)

    // We need a user session handler too to make sure
    // the user is stored in the session between requests
    mainRouter.route().handler(UserSessionHandler.create(oauth2))

    val oauth2Handler = OAuth2AuthHandler.create(oauth2)
//    oauth2Handler.setupCallback(mainRouter.route())
    oauth2Handler.setupCallback(mainRouter.get("/callback"))

    mainRouter.route().handler(oauth2Handler)

    // This cookie handler will be called for all routes
    mainRouter.route().handler(CookieHandler.create())
    mainRouter.route().handler(retrieveCookies)

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
    val mediaRouter = MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel))
    val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel))
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

  /**
    * Extract cookies from request and forward the request to routing again
    */
  private def retrieveCookies(context: RoutingContext)(implicit requestContext: RequestContext): Unit = {
    requestContext.cookies = context.cookies().toSet
    context.next()
  }
}
