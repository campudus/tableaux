package com.campudus.tableaux

import org.vertx.scala.router.Router
import org.vertx.scala.core.VertxAccess
import org.vertx.scala.platform.Container
import org.vertx.scala.core.logging.Logger
import org.vertx.scala.core.Vertx
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.router.routing._
import scala.concurrent.Future
import org.vertx.scala.router.RouterException
import org.vertx.scala.core.json.Json

class TableauxRouter(val container: Container, val logger: Logger, val vertx: Vertx) extends Router with VertxAccess {

  def routes(implicit req: HttpServerRequest): PartialFunction[RouteMatch, Reply] = {
    case Get("/") => SendFile("index.html")
    case Post("/tables") => // create new table
      AsyncReply {
        Future.failed(new RuntimeException("not implemented."))
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