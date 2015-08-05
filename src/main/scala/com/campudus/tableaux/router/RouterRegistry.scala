package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.platform.Verticle
import org.vertx.scala.router.RouterException
import org.vertx.scala.router.routing.{Error, Get, SendFile}

object RouterRegistry {
  def apply(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection): RouterRegistry = {

    val systemModel = SystemModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val structureModel = StructureModel(dbConnection)

    val routers = Seq(
      SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel, structureModel)),
      TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel)),
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel)),
      StructureRouter(tableauxConfig, StructureController(_, structureModel))
    )

    new RouterRegistry(tableauxConfig, routers)
  }
}

class RouterRegistry(override val config: TableauxConfig, val routers: Seq[BaseRouter]) extends BaseRouter {

  override val verticle: Verticle = config.verticle

  override def routes(implicit req: HttpServerRequest): Routing = {
    routers.map(_.routes).foldLeft(defaultRoutes)({
      case (last, current) =>
        last orElse current
    }) orElse noRouteFound
  }

  def defaultRoutes(implicit req: HttpServerRequest): Routing = {
    case Get("/") | Get("/index.html") => SendFile("index.html")
  }

  def noRouteFound(implicit req: HttpServerRequest): Routing = {
    case _ => Error(RouterException(message = "No route found", statusCode = 404))
  }
}
