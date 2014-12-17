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

  val tableIdColumnsIdRowsId = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r
//  val tableIdColumnsIdRow = "/tables/(\\d+)/columns/(\\d+)/row".r
  val tableIdColumnsId = "/tables/(\\d+)/columns/(\\d+)".r
  val tableIdColumns = "/tables/(\\d+)/columns".r
  val tableIdRows = "/tables/(\\d+)/rows".r
  val tableId = "/tables/(\\d+)".r
  val controller = new TableauxController(verticle)

  def routes(implicit req: HttpServerRequest): PartialFunction[RouteMatch, Reply] = {
    case Get("/") => SendFile("index.html")
    case Get(tableId(tableId)) => AsyncReply {
      for {
        j <- controller.getJson(req).map(js => js.putString("action", "getTable").putNumber("tableId", tableId.toLong))
        x <- controller.getTable(j)
      } yield x
    }
    case Get(tableIdColumnsId(tableId, columnId)) => AsyncReply {
      for {
        j <- controller.getJson(req).map(js => js.putString("action", "getColumn").putNumber("tableId", tableId.toLong).putNumber("columnId", columnId.toLong))
        x <- controller.getColumn(j)
      } yield x
    }
    case Post("/tables") =>
      AsyncReply {
        for {
          j <- controller.getJson(req).map(js => js.putString("action", "createTable"))
          x <- controller.createTable(j)
        } yield x
      }
    case Post(tableIdColumns(tableId)) =>
      AsyncReply {
        for {
          j <- controller.getJson(req).map(js => js.putString("action", "createColumn").putNumber("tableId", tableId.toLong))
          x <- controller.createColumn(j)
        } yield x
      }
    case Post(tableIdRows(tableId)) =>
      AsyncReply {
        for {
          j <- controller.getJson(req).map(js => js.putString("action", "createRow").putNumber("tableId", tableId.toLong))
          x <- controller.createRow(j)
        } yield x
      }
    case Post(tableIdColumnsIdRowsId(tableId, columnId, rowId)) =>
      AsyncReply {
        for {
          j <- controller.getJson(req).map(js => js.putString("action", "fillRow").putNumber("tableId", tableId.toLong).putNumber("columnId", columnId.toLong).putNumber("rowId", rowId.toLong))
          x <- controller.fillCell(j)
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
    case Delete(tableId(tableId)) => AsyncReply {
      for {
        j <- controller.getJson(req).map(js => js.putString("action", "deleteTable").putNumber("tableId", tableId.toLong))
        x <- controller.deleteTable(j)
      } yield x
    }
    case Delete(tableIdColumnsId(tableId, columnId)) => AsyncReply {
      for {
        j <- controller.getJson(req).map(js => js.putString("action", "deleteColumn").putNumber("tableId", tableId.toLong).putNumber("columnId", columnId.toLong))
        x <- controller.deleteColumn(j)
      } yield x
    }
  }
}