package com.campudus.tableaux.router

import com.campudus.tableaux.controller.DemoController
import com.campudus.tableaux.database.structure.SetReturn
import com.campudus.tableaux.database.{DatabaseAccess, DatabaseConnection}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.platform.Verticle
import org.vertx.scala.router.routing.Post

class DemoRouter(val verticle: Verticle, val database: DatabaseConnection) extends BaseRouter with DatabaseAccess {

  val controller = new DemoController(verticle, database)

  override def routes(implicit req: HttpServerRequest): Routing = {
    case Post("/reset") => asyncSetReply(controller.resetDB())
    case Post("/resetDemo") => asyncSetReply(controller.createDemoTables())
  }
}
