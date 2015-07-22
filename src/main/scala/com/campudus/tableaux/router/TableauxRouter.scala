package com.campudus.tableaux.router

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.helper.HelperFunctions._
import com.campudus.tableaux.{NoJsonFoundException, TableauxConfig}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.router.routing._

import scala.util.matching.Regex

object TableauxRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => TableauxController): TableauxRouter = {
    new TableauxRouter(config, controllerCurry(config))
  }
}

class TableauxRouter(override val config: TableauxConfig, val controller: TableauxController) extends BaseRouter {

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
    /**
     * Get Rows
     */
    case Get(Rows(tableId)) => asyncGetReply(controller.getRows(tableId.toLong))

    /**
     * Get Row
     */
    case Get(Row(tableId, rowId)) => asyncGetReply(controller.getRow(tableId.toLong, rowId.toLong))

    /**
     * Get Cell
     */
    case Get(Cell(tableId, columnId, rowId)) => asyncGetReply(controller.getCell(tableId.toLong, columnId.toLong, rowId.toLong))

    /**
     * Create Row
     */
    case Post(Rows(tableId)) => asyncSetReply {
      getJson(req) flatMap (json => controller.createRow(tableId.toLong, Some(jsonToSeqOfRowsWithColumnIdAndValue(json)))) recoverWith {
        case _: NoJsonFoundException => controller.createRow(tableId.toLong, None)
      }
    }

    /**
     * Fill Cell
     */
    case Post(Cell(tableId, columnId, rowId)) => asyncSetReply {
      getJson(req) flatMap { json =>
        controller.fillCell(tableId.toLong, columnId.toLong, rowId.toLong, json.getField("value"))
      }
    }

    /**
     * Update Cell
     */
    case Put(Cell(tableId, columnId, rowId)) => asyncSetReply {
      getJson(req) flatMap { json =>
        controller.updateCell(tableId.toLong, columnId.toLong, rowId.toLong, json.getField("value"))
      }
    }

    /**
     * Delete Row
     */
    case Delete(Row(tableId, rowId)) => asyncEmptyReply(controller.deleteRow(tableId.toLong, rowId.toLong))

    /**
     * Delete Attachment
     */
    case Delete(AttachmentOfCell(tableId, columnId, rowId, uuid)) => asyncEmptyReply(controller.deleteAttachment(tableId.toLong, columnId.toLong, rowId.toLong, uuid))
  }
}