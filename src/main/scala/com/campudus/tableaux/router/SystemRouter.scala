package com.campudus.tableaux.router

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.controller.SystemController
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.router.routing.{Get, Post}

object SystemRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => SystemController): SystemRouter = {
    new SystemRouter(config, controllerCurry(config))
  }
}

class SystemRouter(override val config: TableauxConfig, val controller: SystemController) extends BaseRouter {
  override def routes(implicit req: HttpServerRequest): Routing =  {
    case Post("/reset") | Get("/reset") => asyncSetReply(controller.resetDB())
    case Post("/resetDemo") | Get("/resetDemo") => asyncSetReply(controller.createDemoTables())
  }
}