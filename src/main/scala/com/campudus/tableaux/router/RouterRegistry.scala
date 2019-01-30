package com.campudus.tableaux.router

import com.campudus.tableaux.{InvalidRequestException, TableauxConfig}
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import io.vertx.scala.ext.web.{Router, RoutingContext}

object RouterRegistry {

  def apply(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection, router: Router): RouterRegistry = {

    val systemModel = SystemModel(dbConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val attachmentModel = AttachmentModel(dbConnection, fileModel)

    val systemRouter = SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel, structureModel))
//      val tableauxRouter = TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel))
//      val mediaRouter = MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel))
    val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel))
//      val documentationRouter = DocumentationRouter(tableauxConfig)

    val routerRegistry = new RouterRegistry(tableauxConfig)

    router.mountSubRouter("/system", systemRouter.getRouter())

    router.get("/").handler(routerRegistry.defaultRoute)
    router.get("/index.html").handler(systemRouter.defaultRoute)

    router.route().handler(systemRouter.noRouteMatched)

    routerRegistry
  }
}

class RouterRegistry(val config: TableauxConfig) extends BaseRouter {}
