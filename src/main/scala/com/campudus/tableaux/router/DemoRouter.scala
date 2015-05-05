package com.campudus.tableaux.router

import com.campudus.tableaux.helper.HelperFunctions
import HelperFunctions._
import com.campudus.tableaux.controller.{DemoController, TableauxController}
import com.campudus.tableaux.database.{DatabaseAccess, DatabaseConnection, SetReturn}
import org.vertx.scala.core.Vertx
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.logging.Logger
import org.vertx.scala.platform.{Container, Verticle}
import org.vertx.scala.router.Router
import org.vertx.scala.router.routing.Post

class DemoRouter(val verticle: Verticle, val database: DatabaseConnection) extends BaseRouter with DatabaseAccess {

  val controller = new DemoController(verticle, database)

  override def routes(implicit req: HttpServerRequest): Routing = {
    case Post("/reset") => getAsyncReply(SetReturn)(controller.resetDB())
    case Post("/resetDemo") => getAsyncReply(SetReturn)(controller.createDemoTables())
  }
}
