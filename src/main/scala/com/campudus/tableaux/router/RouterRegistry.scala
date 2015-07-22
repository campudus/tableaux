package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{StructureController, MediaController, SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model._
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.platform.Verticle
import org.vertx.scala.router.routing.{Get, SendFile}

object RouterRegistry {
  def apply(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection): RouterRegistry = {

    val systemModel = SystemModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection)
    val folderModel = FolderModel(dbConnection)
    val fileModel = FileModel(dbConnection)
    val structureModel = StructureModel(dbConnection)

    new RouterRegistry(
      tableauxConfig,

      SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel, structureModel)),
      TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel)),
      MediaRouter(tableauxConfig, MediaController(_, folderModel, fileModel)),
      StructureRouter(tableauxConfig, StructureController(_, structureModel))
    )
  }
}

class RouterRegistry(override val config: TableauxConfig,
                     val systemRouter: SystemRouter,
                     val tableauxRouter: TableauxRouter,
                     val mediaRouter: MediaRouter,
                     val structureRouter: StructureRouter) extends BaseRouter {

  override val verticle: Verticle = config.verticle

  override def routes(implicit req: HttpServerRequest): Routing = {
    myRoutes orElse
      systemRouter.routes orElse
      tableauxRouter.routes orElse
      mediaRouter.routes orElse
      structureRouter.routes
  }

  def myRoutes(implicit req: HttpServerRequest): Routing = {
    case Get("/") | Get("/index.html") => SendFile("index.html")
  }
}
