package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.router.auth.TableauxAuthHandler
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import com.typesafe.scalalogging.LazyLogging
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.ext.auth.PubSecKeyOptions
import io.vertx.scala.ext.auth.jwt.{JWTAuth, JWTAuthOptions}
import io.vertx.scala.ext.auth.oauth2.OAuth2Auth
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.handler.{CookieHandler, JWTAuthHandler}
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.collection.mutable

object RouterRegistry extends LazyLogging {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext): Router = {

    val vertx = tableauxConfig.vertx

    val mainRouter: Router = Router.router(vertx)

    implicit val requestContext: RequestContext = RequestContext()

    // This cookie handler will be called for all routes
    mainRouter.route().handler(CookieHandler.create())
    mainRouter.route().handler(retrieveCookies)

    if (!tableauxConfig.authConfig.isEmpty) {

      val pubKey = tableauxConfig.authConfig.getString("realm-public-key")

      val config = JWTAuthOptions()
        .setPubSecKeys(
          mutable.Buffer(
            PubSecKeyOptions()
              .setAlgorithm("RS256")
//              .setPublicKey(
//                "MIGeMA0GCSqGSIb3DQEBAQUAA4GMADCBiAKBgHj2X+CsJG6CXrb7lMmav6e1x6YgEoRYlbxP3GZzfpuvE3DfVP1ZHYGd9OgrlyBIuoCj8Jd28PWar0X9809dS/SpzJVUfobLLQD99Eq4Eu8BxxZYvLwWfGe3kdCRBx5MWPmtvSAI7kDzQei7k2v3BQsK52Oez1alyh7pFifQgR5HAgMBAAE=")))
              .setPublicKey(pubKey)))

//      val jwtAuthProvider = JWTAuth.create(vertx, JWTAuthOptions.fromJson(tableauxConfig.authConfig))
      val jwtAuthProvider = JWTAuth.create(vertx, config)
      val jwtAuthHandler = JWTAuthHandler.create(jwtAuthProvider)

      val authProvider: OAuth2Auth = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
      val authHandler = TableauxAuthHandler(vertx, authProvider, tableauxConfig)
      mainRouter.route().handler(jwtAuthHandler)

      mainRouter.route().handler(authHandler)

//        mainRouter.route().handler(new TableauxAuthHandler2(vertx, authProvider, tableauxConfig))
//        mainRouter.route().handler(new FooHandler(vertx, authProvider, tableauxConfig))
//        mainRouter.route().handler(authHandler.handleExtStuff)
    } else {
      logger.warn(
        "Started WITHOUT access token verification. The API is completely publicly available and NOT secured! This is for development and/or testing purposes ONLY.")
    }

//    var authProvider = JWTAuth.create(vertx, config)
//
//    val jwtAuthHandler = JWTAuthHandler.create(authProvider)
//
//    mainRouter.route().handler(jwtAuthHandler)

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

//  private def authHandler(context: RoutingContext)(implicit requestContext: RequestContext): Unit = {
//    val user = context.user
//
//    user.foreach({
//      case at: AccessToken => {
//        if (at.accessToken == null) {
//          context.fail(401)
//          return
//        } else {
//          println("success babe")
//        }
//      }
//      case _ => context.fail(401)
//    })
////
////    if (user.isInstanceOf[AccessToken]) {
////      val token = user.asInstanceOf[Nothing]
////      if (token.accessToken == null) {
////        rc.fail(401)
////        return
////      }
////      else rc.setUser(syncUser(token.accessToken))
////    }
////    })
//  }

}
