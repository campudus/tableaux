package com.campudus.tableaux.router

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.domain.Pagination
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{NoJsonFoundException, TableauxConfig}
import io.vertx.ext.web.RoutingContext
import org.vertx.scala.router.routing._

import scala.util.matching.Regex

object TableauxRouter {
  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => TableauxController): TableauxRouter = {
    new TableauxRouter(config, controllerCurry(config))
  }
}

class TableauxRouter(override val config: TableauxConfig, val controller: TableauxController) extends BaseRouter {

  private val AttachmentOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/attachment/($uuidRegex)".r
  private val LinkOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/link/(\\d+)".r

  private val Cell: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r

  private val Row: Regex = "/tables/(\\d+)/rows/(\\d+)".r
  private val RowDuplicate: Regex = "/tables/(\\d+)/rows/(\\d+)/duplicate".r
  private val Rows: Regex = "/tables/(\\d+)/rows".r
  private val RowsOfColumn: Regex = "/tables/(\\d+)/columns/(\\d+)/rows".r

  private val CompleteTable: Regex = "/completetable".r
  private val CompleteTableId: Regex = "/completetable/(\\d+)".r

  override def routes(implicit context: RoutingContext): Routing = {

    /**
      * Get Rows
      */
    case Get(Rows(tableId)) => asyncGetReply {
      val limit = getLongParam("limit", context)
      val offset = getLongParam("offset", context)

      val pagination = Pagination(offset, limit)

      controller.retrieveRows(tableId.toLong, pagination)
    }

    /**
      * Get Rows
      */
    case Get(RowsOfColumn(tableId, columnId)) => asyncGetReply {
      val limit = getLongParam("limit", context)
      val offset = getLongParam("offset", context)

      val pagination = Pagination(offset, limit)

      controller.retrieveRows(tableId.toLong, columnId.toLong, pagination)
    }

    /**
      * Get Row
      */
    case Get(Row(tableId, rowId)) => asyncGetReply {
      controller.retrieveRow(tableId.toLong, rowId.toLong)
    }

    /**
      * Get Cell
      */
    case Get(Cell(tableId, columnId, rowId)) => asyncGetReply {
      controller.retrieveCell(tableId.toLong, columnId.toLong, rowId.toLong)
    }

    /**
      * Get complete table
      */
    case Get(CompleteTableId(tableId)) => asyncGetReply {
      controller.retrieveCompleteTable(tableId.toLong)
    }

    /**
      * Create table with columns and rows
      */
    case Post(CompleteTable()) => asyncGetReply {
      for {
        json <- getJson(context)
        completeTable <- if (json.containsKey("rows")) {
          controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), toRowValueSeq(json))
        } else {
          controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), Seq())
        }
      } yield completeTable
    }

    /**
      * Create Row
      */
    case Post(Rows(tableId)) => asyncGetReply {
      for {
        optionalValues <- (for {
          json <- getJson(context)
          option = json.containsKey("columns") && json.containsKey("rows") match {
            case true => Some(toColumnValueSeq(json))
            case false => None
          }
        } yield option) recover {
          case _: NoJsonFoundException => None
        }
        result <- controller.createRow(tableId.toLong, optionalValues)
      } yield result
    }

    /**
      * Duplicate Row
      */
    case Post(RowDuplicate(tableId, rowId)) => asyncGetReply {
      logger.info(s"POST request duplicate mit body=${context.getBodyAsString}")
      controller.duplicateRow(tableId.toLong, rowId.toLong)
    }

    /**
      * Update Cell or add Link/Attachment
      */
    case Post(Cell(tableId, columnId, rowId)) => asyncGetReply {
      for {
        json <- getJson(context)
        updated <- controller.updateCellValue(tableId.toLong, columnId.toLong, rowId.toLong, json.getValue("value"))
      } yield updated
    }

    /**
      * Update Cell or add Link/Attachment
      */
    case Patch(Cell(tableId, columnId, rowId)) => asyncGetReply {
      for {
        json <- getJson(context)
        updated <- controller.updateCellValue(tableId.toLong, columnId.toLong, rowId.toLong, json.getValue("value"))
      } yield updated
    }

    /**
      * Replace Cell value
      */
    case Put(Cell(tableId, columnId, rowId)) => asyncGetReply {
      for {
        json <- getJson(context)
        updated <- controller.replaceCellValue(tableId.toLong, columnId.toLong, rowId.toLong, json.getValue("value"))
      } yield updated
    }

    /**
      * Delete Row
      */
    case Delete(Row(tableId, rowId)) => asyncEmptyReply {
      controller.deleteRow(tableId.toLong, rowId.toLong)
    }

    /**
      * Delete Attachment from Cell
      */
    case Delete(AttachmentOfCell(tableId, columnId, rowId, uuid)) => asyncEmptyReply {
      controller.deleteAttachment(tableId.toLong, columnId.toLong, rowId.toLong, uuid)
    }

    /**
      * Delete Link from Cell
      */
    case Delete(LinkOfCell(tableId, columnId, fromId, toId)) => ???
  }
}