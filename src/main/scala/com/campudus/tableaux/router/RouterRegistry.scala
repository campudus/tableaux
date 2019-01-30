package com.campudus.tableaux.router

import com.campudus.tableaux.{InvalidRequestException, TableauxConfig}
import com.campudus.tableaux.controller.{MediaController, StructureController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import io.vertx.scala.ext.web.{Router, RoutingContext}
import org.vertx.scala.router.RouterException
import org.vertx.scala.router.routing.{Error, Get, SendEmbeddedFile}

import scala.concurrent.Future

object RouterRegistry {

  def apply(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection, router: Router): RouterRegistry = {

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

    val systemRouter = SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel, structureModel))
//      val tableauxRouter = TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel)),
//      val mediaRouter = MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel, attachmentModel)),
//      val structureRouter = StructureRouter(tableauxConfig, StructureController(_, structureModel)),
//      val documentationRouter = DocumentationRouter(tableauxConfig)

    val rr = new RouterRegistry(tableauxConfig, routers)

    router.mountSubRouter("/system", systemRouter.apply())

    router.get("/").handler(rr.defaultRoute)
    router.get("/index.html").handler(rr.defaultRoute)

    router.route().handler(rr.noRouteMatched)

    rr
  }
}

class RouterRegistry(override val config: TableauxConfig, routers: Seq[BaseRouter]) extends BaseRouter {}
