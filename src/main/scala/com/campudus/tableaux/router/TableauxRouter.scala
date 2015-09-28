package com.campudus.tableaux.router

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.domain.Pagination
import com.campudus.tableaux.helper.JsonUtils._
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

  private val AttachmentOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/attachment/($uuidRegex)".r

  private val Cell: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r

  private val Row: Regex = "/tables/(\\d+)/rows/(\\d+)".r
  private val Rows: Regex = "/tables/(\\d+)/rows".r
  private val RowsOfColumn: Regex = "/tables/(\\d+)/columns/(\\d+)/rows".r

  private val CompleteTable: Regex = "/completetable".r
  private val CompleteTableId: Regex = "/completetable/(\\d+)".r

  override def routes(implicit req: HttpServerRequest): Routing = {
    /**
     * Get Rows
     */
    case Get(Rows(tableId)) => asyncGetReply({
      val limit = getLongParam("limit", req)
      val offset = getLongParam("offset", req)

      val pagination = Pagination(offset, limit)

      controller.retrieveRows(tableId.toLong, pagination)
    })

    /**
     * Get Rows
     */
    case Get(RowsOfColumn(tableId, columnId)) => asyncGetReply({
      val limit = getLongParam("limit", req)
      val offset = getLongParam("offset", req)

      val pagination = Pagination(offset, limit)

      controller.retrieveRows(tableId.toLong, columnId.toLong, pagination)
    })

    /**
     * Get Row
     */
    case Get(Row(tableId, rowId)) => asyncGetReply(controller.retrieveRow(tableId.toLong, rowId.toLong))

    /**
     * Get Cell
     */
    case Get(Cell(tableId, columnId, rowId)) => asyncGetReply(controller.retrieveCell(tableId.toLong, columnId.toLong, rowId.toLong))

    /**
     * Get complete table
     */
    case Get(CompleteTableId(tableId)) => asyncGetReply(controller.retrieveCompleteTable(tableId.toLong))

    /**
     * Create table with columns and rows
     */
    case Post(CompleteTable()) => asyncSetReply {
      getJson(req) flatMap { json =>
        if (json.getFieldNames.contains("rows")) {
          controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), toRowValueSeq(json))
        } else {
          controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), Seq())
        }
      }
    }

    /**
     * Create Row
     */
    case Post(Rows(tableId)) => asyncSetReply {
      getJson(req) flatMap (json => controller.createRow(tableId.toLong, Some(toColumnValueSeq(json)))) recoverWith {
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