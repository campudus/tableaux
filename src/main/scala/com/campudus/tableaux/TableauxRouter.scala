package com.campudus.tableaux

import com.campudus.tableaux.database.Table
import org.vertx.scala.router.Router
import org.vertx.scala.core.VertxAccess
import org.vertx.scala.platform.Container
import org.vertx.scala.core.logging.Logger
import org.vertx.scala.core.Vertx
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.router.routing._
import scala.concurrent.{ Future, Promise }
import org.vertx.scala.router.RouterException
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.json.JsonObject
import scala.util.{ Success, Failure }

class TableauxRouter(verticle: Starter) extends Router with VertxAccess {
  val container = verticle.container
  val vertx = verticle.vertx
  val logger = verticle.logger

  val tableIdColumns = "/tables/(\\d+)/columns".r
  val tableId = "/tables/(\\d+)".r
  val controller = new TableauxController(verticle)

  def routes(implicit req: HttpServerRequest): PartialFunction[RouteMatch, Reply] = {
    case Get("/") => SendFile("index.html")
    case Get(tableId(tableId)) => AsyncReply {
      for {
        j <- controller.getJson(req).map(js => js.putString("action", "getTable").putString("tableId", tableId))
        x <- controller.getTable(j)
      } yield x
    }
    case Post("/tables") => // create new table { action: "createTable", name : "Tabelle 1" }
      AsyncReply {
        for {
          j <- controller.getJson(req).map(js => js.putString("action", "createTable"))
          x <- controller.createTable(j)
        } yield x
      }
    case Post(tableIdColumns(tableId)) =>
      AsyncReply {
        for {
          j <- controller.getJson(req).map(js => js.putString("action", "createColumn").putString("tableId", tableId))
          x <- controller.createColumn(j)
        } yield x
      }
    // more posts
    case Get("/tables") => // list all tables
      AsyncReply {
        Future.successful(
          Ok(
            Json.obj(
              "id" -> 123,
              "name" -> "Personen",
              "cols" -> Json.arr(
                Json.obj("id" -> 1, "name" -> "Vorname"),
                Json.obj("id" -> 2, "name" -> "Wohnort")),
              "rows" -> Json.arr(
                Json.obj("id" -> 1, "c1" -> "Peter", "c2" -> "Berlin"),
                Json.obj("id" -> 2, "c1" -> "Kurt", "c2" -> "Düsseldorf")))))
      }
  }
}