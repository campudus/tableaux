package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.{TableauxJwtAuthHandler, TableauxKeycloakAuthHandler}
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import com.typesafe.scalalogging.LazyLogging
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.ext.auth.jwt.{JWTAuth, JWTAuthOptions}
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.{JWTAuthHandler, OAuth2AuthHandler}

object RouterRegistry extends LazyLogging {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext): Router = {

    val vertx = tableauxConfig.vertx

    val mainRouter: Router = Router.router(vertx)

    implicit val requestContext: RequestContext = RequestContext()

    if (!tableauxConfig.authConfig.isEmpty) {

//      // #####################
//      // #### THE JWT Way ####
//      // #####################
//
//      "auth": {
//        "pubSecKeys": [
//      {
//        "algorithm": "RS256",
//        "publicKey": "MIGeMA0GCSqGSIb3DQEBAQUAA4GMADCBiAKBgHj2X+CsJG6CXrb7lMmav6e1x6YgEoRYlbxP3GZzfpuvE3DfVP1ZHYGd9OgrlyBIuoCj8Jd28PWar0X9809dS/SpzJVUfobLLQD99Eq4Eu8BxxZYvLwWfGe3kdCRBx5MWPmtvSAI7kDzQei7k2v3BQsK52Oez1alyh7pFifQgR5HAgMBAAE="
//      }, {
//        "algorithm": "RS256",
//        "publicKey": "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCrVrCuTtArbgaZzL1hvh0xtL5mc7o0NqPVnYXkLvgcwiC3BjLGw1tGEGoJaXDuSaRllobm53JBhjx33UNv+5z/UMG4kytBWxheNVKnL6GgqlNabMaFfPLPCF8kAgKnsi79NMo+n6KnSY8YeUmec/p2vjO2NjsSAVcWEQMVhJ31LwIDAQAB"
//      }
//        ],
//        "audience": "grud-backend",
//        "issuer": "campudus-test"
//      }
//
//      val jwtOptions = JWTAuthOptions.fromJson(tableauxConfig.authConfig)
//      val jwtAuthProvider: JWTAuth = JWTAuth.create(vertx, jwtOptions)
//      val jwtAuthHandler: JWTAuthHandler = JWTAuthHandler.create(jwtAuthProvider)
//
//      mainRouter.route().handler(jwtAuthHandler)
//
//      // validate additional claims
//      val tableauxJwtAuthHandler = new TableauxJwtAuthHandler(vertx, tableauxConfig)
//      mainRouter.route().handler(tableauxJwtAuthHandler)

      // ##########################
      // #### THE Keycloak Way ####
      // ##########################
//
//      "auth": {
//        "realm": "keycloak-realm-permission-test",
//        "realm-public-key": "MIGeMA0GCSqGSIb3DQEBAQUAA4GMADCBiAKBgHj2X+CsJG6CXrb7lMmav6e1x6YgEoRYlbxP3GZzfpuvE3DfVP1ZHYGd9OgrlyBIuoCj8Jd28PWar0X9809dS/SpzJVUfobLLQD99Eq4Eu8BxxZYvLwWfGe3kdCRBx5MWPmtvSAI7kDzQei7k2v3BQsK52Oez1alyh7pFifQgR5HAgMBAAE=",
//        "bearer-only": true,
//        "auth-server-url": "http://localhost:9999/auth",
//        "ssl-required": "external",
//        "resource": "grud-backend",
//        "verify-token-audience": false,
//        "use-resource-role-mappings": false,
//        "confidential-port": 0,
//
//        "audience": "grud-backend",
//        "issuer": "campudus-test"
//      }

      val keycloakAuthProvider = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
      val keycloakAuthHandler = OAuth2AuthHandler.create(keycloakAuthProvider)
      mainRouter.route().handler(keycloakAuthHandler)

      val tableauxKeycloakAuthHandler = new TableauxKeycloakAuthHandler(vertx, tableauxConfig)
      mainRouter.route().handler(tableauxKeycloakAuthHandler)

    } else {
      logger.warn(
        "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! This is for development and/or testing purposes ONLY.")
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

    mainRouter.route().handler(systemRouter.noRouteMatched)

    mainRouter
  }
}
