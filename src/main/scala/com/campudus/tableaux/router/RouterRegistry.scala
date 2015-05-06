package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.{SystemController, TableauxController}
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.{SystemModel, TableauxModel}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.platform.Verticle
import org.vertx.scala.router.routing.{Get, SendFile}

object RouterRegistry {
  def apply(tableauxConfig: TableauxConfig, dbConnection: DatabaseConnection): RouterRegistry = {
    val systemModel = SystemModel(dbConnection)

    val tableauxModel = TableauxModel(dbConnection)

    new RouterRegistry(
      tableauxConfig,
      SystemRouter(tableauxConfig, SystemController(_, systemModel, tableauxModel)),
      TableauxRouter(tableauxConfig, TableauxController(_, tableauxModel))
    )
  }
}

//TODO I would like to use a trait instead but that doesn't work
class RouterRegistry(
                      override val config: TableauxConfig,
                      val systemRouter: SystemRouter,
                      val tableauxRouter: TableauxRouter
                      ) extends BaseRouter {

  override val verticle: Verticle = config.verticle

  override def routes(implicit req: HttpServerRequest): Routing = {
    myRoutes orElse
      systemRouter.routes orElse
      tableauxRouter.routes
  }

  def myRoutes(implicit req: HttpServerRequest): Routing = {
    case Get("/") | Get("/index.html") => SendFile("index.html")
  }
}
