package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.lang.scala.json.JsonObject
import io.vertx.scala.ext.auth.oauth2.OAuth2Auth
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.handler.{CookieHandler, OAuth2AuthHandler}
import io.vertx.scala.ext.web.{Router, RoutingContext}
import org.vertx.scala.core.json.Json

import scala.util.{Failure, Success}

object RouterRegistry {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection): Router = {

    val mainRouter: Router = Router.router(tableauxConfig.vertx)

    implicit val requestContext: RequestContext = RequestContext()

    // you would get this config from the keycloak admin console
    var keycloakJson = Json.fromObjectString(
      """
        |{
        |  "realm": "master",
        |  "auth-server-url": "http://localhost:9999/auth",
        |  "ssl-required": "external",
        |  "resource": "grud",
        |  "credentials": {
        |    "secret": "bb497817-294e-4abd-b34c-307ea6c36a64"
        |  },
        |  "confidential-port": 0
        |}
      """.stripMargin
    )

    // Initialize the OAuth2 Library
//    var oauth2 = KeycloakAuth.create(tableauxConfig.vertx, OAuth2FlowType.PASSWORD, keycloakJson)

//    // first get a token (authenticate)
//    oauth2
//      .authenticateFuture(new io.vertx.core.json.JsonObject().put("username", "user").put("password", "secret"))
//      .onComplete {
//        case Success(result) => {}
//        case Failure(cause) => {
//          println(s"$cause")
//        }
//      }

    val oauth2 = OAuth2AuthHandler.create(
      OAuth2Auth.createKeycloak(tableauxConfig.vertx, OAuth2FlowType.AUTH_CODE, keycloakJson),
      "http://localhost:8080/docs")

    oauth2.setupCallback(mainRouter.get("/docs"))

    mainRouter.route().handler(oauth2)

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
