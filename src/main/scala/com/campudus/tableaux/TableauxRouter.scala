package com.campudus.tableaux

import com.campudus.tableaux.database.Table
import com.campudus.tableaux.TableauxController._
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
import scala.util.{Success, Failure}

class TableauxRouter(val container: Container, val logger: Logger, val vertx: Vertx) extends Router with VertxAccess {

  //val exp = "/column/\\w+".r
  
  def routes(implicit req: HttpServerRequest): PartialFunction[RouteMatch, Reply] = {
    case Get("/") => SendFile("index.html")
    case Post("/tables") => // create new table { action: "createTable", name : "Tabelle 1" }
      AsyncReply {
        for{
          j <- getJson(req).map(js => js.putString("action", "createTable"))
          x <- createTable(j, vertx)
        } yield x
      }
    case Post("/columns") =>
      AsyncReply {
        for{
          j <- getJson(req).map(js => js.putString("action", "createColumn"))
          x <- createStringColumn(j)
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