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
    case Get(Tables()) => asyncGetReply(controller.getAllTables())
    case Get(Table(tableId)) => asyncGetReply(controller.getTable(tableId.toLong))
    case Get(CompleteTable(tableId)) => asyncGetReply(controller.getCompleteTable(tableId.toLong))
    case Get(Columns(tableId)) => asyncGetReply(controller.getColumns(tableId.toLong))
    case Get(Column(tableId, columnId)) => asyncGetReply(controller.getColumn(tableId.toLong, columnId.toLong))
    case Get(Rows(tableId)) => asyncGetReply(controller.getRows(tableId.toLong))
    case Get(Row(tableId, rowId)) => asyncGetReply(controller.getRow(tableId.toLong, rowId.toLong))
    case Get(Cell(tableId, columnId, rowId)) => asyncGetReply(controller.getCell(tableId.toLong, columnId.toLong, rowId.toLong))

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