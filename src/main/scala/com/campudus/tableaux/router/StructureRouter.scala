package com.campudus.tableaux.router

import com.campudus.tableaux.controller.{StructureController, TableauxController}
import com.campudus.tableaux.helper.HelperFunctions._
import com.campudus.tableaux.{NoJsonFoundException, TableauxConfig}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.router.routing._

import scala.util.matching.Regex

object StructureRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => StructureController): StructureRouter = {
    new StructureRouter(config, controllerCurry(config))
  }
}

class StructureRouter(override val config: TableauxConfig, val controller: StructureController) extends BaseRouter {

  val AttachmentOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/attachment/($uuidRegex)".r

  val Cell: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r
  
  val Column: Regex = "/tables/(\\d+)/columns/(\\d+)".r
  val Columns: Regex = "/tables/(\\d+)/columns".r
  
  val Row: Regex = "/tables/(\\d+)/rows/(\\d+)".r
  val Rows: Regex = "/tables/(\\d+)/rows".r
  
  val Table: Regex = "/tables/(\\d+)".r
  val Tables: Regex = "/tables".r

  val CompleteTable: Regex = "/completetable/(\\d+)".r

  override def routes(implicit req: HttpServerRequest): Routing = {
    case Get(Tables()) => asyncGetReply(controller.getAllTables())
    case Get(Table(tableId)) => asyncGetReply(controller.getTable(tableId.toLong))
    case Get(CompleteTable(tableId)) => asyncGetReply(controller.getCompleteTable(tableId.toLong))
    case Get(Columns(tableId)) => asyncGetReply(controller.getColumns(tableId.toLong))
    case Get(Column(tableId, columnId)) => asyncGetReply(controller.getColumn(tableId.toLong, columnId.toLong))

    /**
     * Create Table
     */
    case Post(Tables()) => asyncSetReply {
      getJson(req) flatMap { json =>
        if (json.getFieldNames.contains("columns")) {
          if (json.getFieldNames.contains("rows")) {
            controller.createTable(json.getString("name"), jsonToSeqOfColumnNameAndType(json), jsonToSeqOfRowsWithValue(json))
          } else {
            controller.createTable(json.getString("name"), jsonToSeqOfColumnNameAndType(json), Seq())
          }
        } else {
          controller.createTable(json.getString("name"))
        }
      }
    }

    /**
     * Create Column
     */
    case Post(Columns(tableId)) => asyncSetReply {
      getJson(req) flatMap (json => controller.createColumn(tableId.toLong, jsonToSeqOfColumnNameAndType(json)))
    }

    /**
     * Change Table
     */
    case Post(Table(tableId)) => asyncEmptyReply {
      getJson(req) flatMap { json =>
        controller.changeTableName(tableId.toLong, json.getString("name"))
      }
    }

    /**
     * Change Column
     */
    case Post(Column(tableId, columnId)) => asyncEmptyReply {
      getJson(req) flatMap {
        json =>
          val (optName, optOrd, optKind) = getColumnChanges(json)
          controller.changeColumn(tableId.toLong, columnId.toLong, optName, optOrd, optKind)
      }
    }

    /**
     * Delete Table
     */
    case Delete(Table(tableId)) => asyncEmptyReply(controller.deleteTable(tableId.toLong))

    /**
     * Delete Column
     */
    case Delete(Column(tableId, columnId)) => asyncEmptyReply(controller.deleteColumn(tableId.toLong, columnId.toLong))
  }
}