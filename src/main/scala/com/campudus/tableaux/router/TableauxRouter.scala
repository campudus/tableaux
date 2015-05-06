package com.campudus.tableaux.router

import com.campudus.tableaux.database.structure.{EmptyReturn, SetReturn, GetReturn}
import com.campudus.tableaux.helper.HelperFunctions
import HelperFunctions._
import com.campudus.tableaux.NoJsonFoundException
import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database._
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.platform.Verticle
import org.vertx.scala.router.routing._

import scala.util.matching.Regex

class TableauxRouter(val verticle: Verticle, val databaseAddress: String) extends BaseRouter with DatabaseAccess {
  val TableIdColumnsIdRowsId: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r
  val TableIdColumnsId: Regex = "/tables/(\\d+)/columns/(\\d+)".r
  val TableIdColumns: Regex = "/tables/(\\d+)/columns".r
  val TableIdRowsId: Regex = "/tables/(\\d+)/rows/(\\d+)".r
  val TableIdRows: Regex = "/tables/(\\d+)/rows".r
  val TableId: Regex = "/tables/(\\d+)".r

  val database = new DatabaseConnection(verticle, databaseAddress)
  val controller = new TableauxController(verticle, database)

  val demoRouter = new DemoRouter(verticle, database)

  override def routes(implicit request: HttpServerRequest):  Routing = {
    index orElse
      demoRouter.routes orElse
      this.myRoutes
  }

  def index(implicit request: HttpServerRequest): Routing = {
    case Get("/") => SendFile("index.html")
    case Get("/index.html") => SendFile("index.html")
  }

  def myRoutes(implicit req: HttpServerRequest): Routing = {
    case Get("/tables") => asyncGetReply(controller.getAllTables())
    case Get(TableId(tableId)) => asyncGetReply(controller.getTable(tableId.toLong))
    case Get(TableIdColumnsId(tableId, columnId)) => asyncGetReply(controller.getColumn(tableId.toLong, columnId.toLong))
    case Get(TableIdRowsId(tableId, rowId)) => asyncGetReply(controller.getRow(tableId.toLong, rowId.toLong))
    case Get(TableIdColumnsIdRowsId(tableId, columnId, rowId)) => asyncGetReply(controller.getCell(tableId.toLong, columnId.toLong, rowId.toLong))

    case Post("/tables") => asyncSetReply {
      getJson(req) flatMap { json =>
        if (json.getFieldNames.contains("columns")) {
          if (json.getFieldNames.contains("rows")) {
            controller.createTable(json.getString("tableName"), jsonToSeqOfColumnNameAndType(json), jsonToSeqOfRowsWithValue(json))
          } else {
            controller.createTable(json.getString("tableName"), jsonToSeqOfColumnNameAndType(json), Seq())
          }
        } else {
          controller.createTable(json.getString("tableName"))
        }
      }
    }
    case Post(TableIdColumns(tableId)) => asyncSetReply {
      getJson(req) flatMap (json => controller.createColumn(tableId.toLong, jsonToSeqOfColumnNameAndType(json)))
    }
    case Post(TableIdRows(tableId)) => asyncSetReply {
      getJson(req) flatMap (json => controller.createRow(tableId.toLong, Some(jsonToSeqOfRowsWithColumnIdAndValue(json)))) recoverWith {
        case _: NoJsonFoundException => controller.createRow(tableId.toLong, None)
      }
    }
    case Post(TableIdColumnsIdRowsId(tableId, columnId, rowId)) => asyncSetReply {
      getJson(req) flatMap {
        json => controller.fillCell(tableId.toLong, columnId.toLong, rowId.toLong, jsonToValues(json))
      }
    }
    case Post(TableId(tableId)) => asyncEmptyReply(getJson(req) flatMap (json => controller.changeTableName(tableId.toLong, json.getString("tableName"))))
    case Post(TableIdColumnsId(tableId, columnId)) => asyncEmptyReply {
      getJson(req) flatMap {
        json =>
          val (optName, optOrd, optKind) = getColumnChanges(json)
          controller.changeColumn(tableId.toLong, columnId.toLong, optName, optOrd, optKind)
      }
    }

    case Delete(TableId(tableId)) => asyncEmptyReply(controller.deleteTable(tableId.toLong))
    case Delete(TableIdColumnsId(tableId, columnId)) => asyncEmptyReply(controller.deleteColumn(tableId.toLong, columnId.toLong))
    case Delete(TableIdRowsId(tableId, rowId)) => asyncEmptyReply(controller.deleteRow(tableId.toLong, rowId.toLong))
  }
}