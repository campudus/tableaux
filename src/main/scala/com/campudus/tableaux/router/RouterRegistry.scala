package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import com.campudus.tableaux.{RequestContext, TableauxConfig}
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.ext.auth.User
import io.vertx.scala.ext.auth.oauth2.OAuth2Auth
import io.vertx.scala.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.scala.ext.web.handler.{CookieHandler, OAuth2AuthHandler}
import io.vertx.scala.ext.web.{Router, RoutingContext}
import org.vertx.scala.core.json.Json

object RouterRegistry {

  def init(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection)(
      implicit ec: VertxExecutionContext): Router = {

    val vertx = tableauxConfig.vertx

    val mainRouter: Router = Router.router(vertx)

    implicit val requestContext: RequestContext = RequestContext()

    // This cookie handler will be called for all routes
    mainRouter.route().handler(CookieHandler.create())
    mainRouter.route().handler(retrieveCookies)

    val oauth2: OAuth2Auth = KeycloakAuth.create(vertx, tableauxConfig.keycloakConfig)

    oauth2
      .loadJWKFuture()
      .onComplete(res => {
        if (res.isSuccess) {
          println("XXX: loadJWK success")
          val jwk = res.get
          println("XXX: " + jwk)
        } else {
          println("XXX: loadJWK failed!")
        }
      })

    val oauth2Handler = OAuth2AuthHandler.create(oauth2)

    mainRouter.route().handler(oauth2Handler)

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
    val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel), oauth2)
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
  private def myHandler(context: RoutingContext)(implicit requestContext: RequestContext): Unit = {
    requestContext.cookies = context.cookies().toSet
    context.next()
  }

  /**
    * Extract cookies from request and forward the request to routing again
    */
  private def retrieveCookies(context: RoutingContext)(implicit requestContext: RequestContext): Unit = {
    requestContext.cookies = context.cookies().toSet
    context.next()
  }

  private def logout(context: RoutingContext)(implicit requestContext: RequestContext): Unit = {
    println("XXX: LOGGING OUT!!!")

    val userOpt: Option[User] = context.user

    userOpt.foreach({
      println("XXX: clearCache()")
      _.clearCache()
    })

    if (userOpt.isDefined) {
//      val user1: AccessToken = userOpt.get.asInstanceOf[AccessToken]
      val user1: User = userOpt.get

//      println("XXX: " + user1.tokenType())
      println("XXX: " + user1.principal())
      user1.clearCache()

//      user1.logout(res => {
//        if (res.succeeded()) {
//          println("XXX2: LOGGED OUT!!!")
//        } else {
//          println("XXX3: CAN NOT LOG OUT!!!")
//        }
//      })

//      for {
//        _ <- user1.logoutFuture()
//      } yield ()

//      context.session().foreach(_.destroy())
//      context.session().foreach(_.setAccessed())
//
//      context.cookies().clear()
//
//      context.user().map(_.clearCache())
//      context.user().map(_.principal().clear())

      println("XXX2: LOGGED OUT!!!")

    }
//    context.reroute("/")
    context.response.putHeader("location", "/").setStatusCode(302).end
  }

}
