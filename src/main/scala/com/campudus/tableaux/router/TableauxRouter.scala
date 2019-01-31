package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.domain.{CellAnnotationType, Pagination}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{InvalidJsonException, NoJsonFoundException, TableauxConfig}
import org.vertx.scala.core.json.JsonArray
import io.vertx.scala.ext.web.RoutingContext

import scala.concurrent.Future
import scala.util.matching.Regex

object TableauxRouter {

  def apply(config: TableauxConfig, controllerCurry: (TableauxConfig) => TableauxController): TableauxRouter = {
    new TableauxRouter(config, controllerCurry(config))
  }
}

class TableauxRouter(override val config: TableauxConfig, val controller: TableauxController) extends BaseRouter {

  private val AttachmentOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/attachment/($uuidRegex)".r
  private val LinkOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/link/(\\d+)".r
  private val LinkOrderOfCell: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/link/(\\d+)/order".r

  private val ColumnsValues: Regex = "/tables/(\\d+)/columns/(\\d+)/values".r
  private val ColumnsValuesWithLangtag: Regex = s"/tables/(\\d+)/columns/(\\d+)/values/($langtagRegex)".r

  private val Cell: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)".r
  private val CellAnnotations: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/annotations".r
  private val CellAnnotation: Regex = s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/annotations/($uuidRegex)".r
  private val CellAnnotationLangtag: Regex =
    s"/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/annotations/($uuidRegex)/($langtagRegex)".r

  private val Row: Regex = "/tables/(\\d+)/rows/(\\d+)".r
  private val RowDuplicate: Regex = "/tables/(\\d+)/rows/(\\d+)/duplicate".r
  private val RowDependent: Regex = "/tables/(\\d+)/rows/(\\d+)/dependent".r
  private val RowAnnotations: Regex = "/tables/(\\d+)/rows/(\\d+)/annotations".r
  private val Rows: Regex = "/tables/(\\d+)/rows".r
  private val RowsAnnotations: Regex = "/tables/(\\d+)/rows/annotations".r
  private val RowsOfColumn: Regex = "/tables/(\\d+)/columns/(\\d+)/rows".r
  private val RowsOfFirstColumn: Regex = "/tables/(\\d+)/columns/first/rows".r
  private val RowsOfLinkCell: Regex = "/tables/(\\d+)/columns/(\\d+)/rows/(\\d+)/foreignRows".r

  private val CompleteTable: Regex = "/completetable".r
  private val CompleteTableId: Regex = "/completetable/(\\d+)".r

  private val AnnotationsTable: Regex = "/tables/(\\d+)/annotations".r

  private val AnnotationCount: Regex = "/tables/annotationCount".r

  private val TranslationStatus: Regex = "/tables/translationStatus".r

//  override def routes(implicit context: RoutingContext): Routing = {
//
//    /**
//      * Get Rows
//      */
//    case Get(Rows(tableId)) =>
//      asyncGetReply {
//        val limit = getLongParam("limit", context)
//        val offset = getLongParam("offset", context)
//
//        val pagination = Pagination(offset, limit)
//
//        controller.retrieveRows(tableId.toLong, pagination)
//      }
//
//    /**
//      * Get foreign rows from a link cell point of view
//      * e.g. cardinality in both direction will be considered
//      */
//    case Get(RowsOfLinkCell(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        val limit = getLongParam("limit", context)
//        val offset = getLongParam("offset", context)
//
//        val pagination = Pagination(offset, limit)
//
//        controller.retrieveForeignRows(tableId.toLong, columnId.toLong, rowId.toLong, pagination)
//      }
//
//    /**
//      * Get rows of column
//      */
//    case Get(RowsOfColumn(tableId, columnId)) =>
//      asyncGetReply {
//        val limit = getLongParam("limit", context)
//        val offset = getLongParam("offset", context)
//
//        val pagination = Pagination(offset, limit)
//
//        controller.retrieveRowsOfColumn(tableId.toLong, columnId.toLong, pagination)
//      }
//
//    /**
//      * Get rows of first column
//      */
//    case Get(RowsOfFirstColumn(tableId)) =>
//      asyncGetReply {
//        val limit = getLongParam("limit", context)
//        val offset = getLongParam("offset", context)
//
//        val pagination = Pagination(offset, limit)
//
//        controller.retrieveRowsOfFirstColumn(tableId.toLong, pagination)
//      }
//
//    /**
//      * Get Row
//      */
//    case Get(Row(tableId, rowId)) =>
//      asyncGetReply {
//        controller.retrieveRow(tableId.toLong, rowId.toLong)
//      }
//
//    /**
//      * Get dependent rows
//      */
//    case Get(RowDependent(tableId, rowId)) =>
//      asyncGetReply {
//        controller.retrieveDependentRows(tableId.toLong, rowId.toLong)
//      }
//
//    /**
//      * Get Cell
//      */
//    case Get(Cell(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        controller.retrieveCell(tableId.toLong, columnId.toLong, rowId.toLong)
//      }
//
//    /**
//      * Get complete table
//      */
//    case Get(CompleteTableId(tableId)) =>
//      asyncGetReply {
//        controller.retrieveCompleteTable(tableId.toLong)
//      }
//
//    /**
//      * Create table with columns and rows
//      */
//    case Post(CompleteTable()) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          completeTable <- if (json.containsKey("rows")) {
//            controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), toRowValueSeq(json))
//          } else {
//            controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), Seq())
//          }
//        } yield completeTable
//      }
//
//    /**
//      * Create Row
//      */
//    case Post(Rows(tableId)) =>
//      asyncGetReply {
//        for {
//          optionalValues <- (for {
//            json <- getJson(context)
//            option = if (json.containsKey("columns") && json.containsKey("rows")) {
//              Some(toColumnValueSeq(json))
//            } else {
//              None
//            }
//          } yield option) recover {
//            case _: NoJsonFoundException => None
//          }
//          result <- controller.createRow(tableId.toLong, optionalValues)
//        } yield result
//      }
//
//    /**
//      * Duplicate Row
//      */
//    case Post(RowDuplicate(tableId, rowId)) =>
//      asyncGetReply {
//        controller.duplicateRow(tableId.toLong, rowId.toLong)
//      }
//
//    /**
//      * Update Row Annotations
//      */
//    case Patch(RowAnnotations(tableId, rowId)) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          finalFlagOpt = booleanToValueOption(json.containsKey("final"), json.getBoolean("final", false))
//            .map(_.booleanValue())
//
//          updated <- controller.updateRowAnnotations(tableId.toLong, rowId.toLong, finalFlagOpt)
//        } yield updated
//      }
//
//    /**
//      * Update Row Annotations
//      */
//    case Patch(RowsAnnotations(tableId)) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          finalFlagOpt = booleanToValueOption(json.containsKey("final"), json.getBoolean("final", false))
//            .map(_.booleanValue())
//
//          updated <- controller.updateRowsAnnotations(tableId.toLong, finalFlagOpt)
//        } yield updated
//      }
//
//    /**
//      * Add Cell Annotation (will possibly be merged with an existing annotation)
//      */
//    case Post(CellAnnotations(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        import com.campudus.tableaux.ArgumentChecker._
//
//        for {
//          json <- getJson(context)
//
//          langtags = checked(asCastedList[String](json.getJsonArray("langtags", new JsonArray())))
//          flagType = checked(hasString("type", json).map(CellAnnotationType(_)))
//          value = json.getString("value")
//
//          cellAnnotation <- controller
//            .addCellAnnotation(tableId.toLong, columnId.toLong, rowId.toLong, langtags, flagType, value)
//        } yield cellAnnotation
//      }
//
//    /**
//      * Delete Cell Annotation
//      */
//    case Delete(CellAnnotation(tableId, columnId, rowId, uuid)) =>
//      asyncGetReply {
//        controller.deleteCellAnnotation(tableId.toLong, columnId.toLong, rowId.toLong, UUID.fromString(uuid))
//      }
//
//    /**
//      * Delete Langtag from Cell Annotation
//      */
//    case Delete(CellAnnotationLangtag(tableId, columnId, rowId, uuid, langtag)) =>
//      asyncGetReply {
//        controller.deleteCellAnnotation(tableId.toLong, columnId.toLong, rowId.toLong, UUID.fromString(uuid), langtag)
//      }
//
//    /**
//      * Retrieve all Cell Annotations for a specific table
//      */
//    case Get(AnnotationsTable(tableId)) =>
//      asyncGetReply {
//        controller.retrieveTableWithCellAnnotations(tableId.toLong)
//      }
//
//    /**
//      * Retrieve Cell Annotation count for all tables
//      */
//    case Get(AnnotationCount()) =>
//      asyncGetReply {
//        controller.retrieveTablesWithCellAnnotationCount()
//      }
//
//    /**
//      * Retrieve translation status for all tables
//      */
//    case Get(TranslationStatus()) =>
//      asyncGetReply {
//        controller.retrieveTranslationStatus()
//      }
//
//    /**
//      * Update Cell or add Link/Attachment
//      */
//    case Post(Cell(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          updated <- controller.updateCellValue(tableId.toLong, columnId.toLong, rowId.toLong, json.getValue("value"))
//        } yield updated
//      }
//
//    /**
//      * Update Cell or add Link/Attachment
//      */
//    case Patch(Cell(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          updated <- controller.updateCellValue(tableId.toLong, columnId.toLong, rowId.toLong, json.getValue("value"))
//        } yield updated
//      }
//
//    /**
//      * Replace Cell value
//      */
//    case Put(Cell(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          updated <- if (json.containsKey("value")) {
//            controller.replaceCellValue(tableId.toLong, columnId.toLong, rowId.toLong, json.getValue("value"))
//          } else {
//            Future.failed(InvalidJsonException("request must contain a value", "value_is_missing"))
//          }
//        } yield updated
//      }
//
//    /**
//      * Change order of link
//      */
//    case Put(LinkOrderOfCell(tableId, columnId, rowId, toId)) =>
//      asyncGetReply {
//        for {
//          json <- getJson(context)
//          updated <- controller
//            .updateCellLinkOrder(tableId.toLong, columnId.toLong, rowId.toLong, toId.toLong, toLocationType(json))
//        } yield updated
//      }
//
//    /**
//      * Delete Row
//      */
//    case Delete(Row(tableId, rowId)) =>
//      asyncEmptyReply {
//        controller.deleteRow(tableId.toLong, rowId.toLong)
//      }
//
//    /**
//      * Clear Cell value
//      */
//    case Delete(Cell(tableId, columnId, rowId)) =>
//      asyncGetReply {
//        controller.clearCellValue(tableId.toLong, columnId.toLong, rowId.toLong)
//      }
//
//    /**
//      * Delete Attachment from Cell
//      */
//    case Delete(AttachmentOfCell(tableId, columnId, rowId, uuid)) =>
//      asyncEmptyReply {
//        controller.deleteAttachment(tableId.toLong, columnId.toLong, rowId.toLong, uuid)
//      }
//
//    /**
//      * Delete Link from Cell
//      */
//    case Delete(LinkOfCell(tableId, columnId, rowId, toId)) =>
//      asyncGetReply {
//        controller.deleteLink(tableId.toLong, columnId.toLong, rowId.toLong, toId.toLong)
//      }
//
//    /**
//      * Retrieve unique values of a shorttext column
//      */
//    case Get(ColumnsValues(tableId, columnId)) =>
//      asyncGetReply {
//        controller.retrieveColumnValues(tableId.toLong, columnId.toLong, None)
//      }
//
//    /**
//      * Retrieve unique values of a multi-language shorttext column
//      */
//    case Get(ColumnsValuesWithLangtag(tableId, columnId, langtag)) =>
//      asyncGetReply {
//        controller.retrieveColumnValues(tableId.toLong, columnId.toLong, Some(langtag))
//      }
//  }
}
