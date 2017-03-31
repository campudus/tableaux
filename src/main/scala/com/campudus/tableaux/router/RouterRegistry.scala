package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import io.vertx.ext.web.RoutingContext
import io.vertx.scala.ScalaVerticle
import org.vertx.scala.router.RouterException
import org.vertx.scala.router.routing.{Error, Get, SendEmbeddedFile}

object RouterRegistry {
  def apply(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection): RouterRegistry = {

    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection, fileModel)

    val routers = Seq(
      SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel, structureModel)),
      TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel)),
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel)),
      StructureRouter(tableauxConfig, StructureController(_, structureModel)),
      DocumentationRouter(tableauxConfig)
    )

    new RouterRegistry(tableauxConfig, routers)
  }
}

class RouterRegistry(override val config: TableauxConfig, val routers: Seq[BaseRouter]) extends BaseRouter {

  override val verticle: ScalaVerticle = config.verticle

  override def routes(implicit context: RoutingContext): Routing = {
    routers.map(_.routes).foldLeft(defaultRoutes)({
      case (last, current) =>
        last orElse current
    }) orElse noRouteFound
  }

  private def defaultRoutes(implicit context: RoutingContext): Routing = {
    case Get("/") | Get("/index.html") => SendEmbeddedFile("/index.html")
  }

  private def noRouteFound(implicit context: RoutingContext): Routing = {
    case _ => Error(RouterException(message = s"No route found for path ${context.request().method().toString} ${context.normalisedPath()}", id = "NOT FOUND", statusCode = 404))
  }
}
