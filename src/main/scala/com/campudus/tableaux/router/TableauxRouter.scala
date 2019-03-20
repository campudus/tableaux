package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.domain.{CellAnnotationType, Pagination}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{InvalidJsonException, NoJsonFoundException, TableauxConfig}
import io.vertx.scala.ext.web.handler.BodyHandler
import io.vertx.scala.ext.web.{Router, RoutingContext}
import org.vertx.scala.core.json.JsonArray

import scala.concurrent.Future
import scala.util.Try

object TableauxRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => TableauxController): TableauxRouter = {
    new TableauxRouter(config, controllerCurry(config))
  }
}

class TableauxRouter(override val config: TableauxConfig, val controller: TableauxController) extends BaseRouter {

  private val AttachmentOfCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/attachment/$uuidRegex"""
  private val LinkOfCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/link/$LINK_ID"""
  private val LinkOrderOfCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/link/$LINK_ID/order"""

  private val ColumnsValues: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/values"""
  private val ColumnsValuesWithLangtag: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/values/$langtagRegex"""

  private val Cell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID"""
  private val CellAnnotations: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/annotations"""
  private val CellAnnotation: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/annotations/$uuidRegex"""
  private val CellAnnotationLangtag: String =
    s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/annotations/$uuidRegex/$langtagRegex"""

  private val Row: String = s"""/tables/$TABLE_ID/rows/$ROW_ID"""
  private val RowDuplicate: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/duplicate"""
  private val RowDependent: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/dependent"""
  private val RowAnnotations: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/annotations"""
  private val Rows: String = s"""/tables/$TABLE_ID/rows"""
  private val RowsAnnotations: String = s"""/tables/$TABLE_ID/rows/annotations"""
  private val RowsOfColumn: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows"""
  private val RowsOfFirstColumn: String = s"""/tables/$TABLE_ID/columns/first/rows"""
  private val RowsOfLinkCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/foreignRows"""

  private val CompleteTable: String = s"""/completetable"""
  private val CompleteTableId: String = s"""/completetable/$TABLE_ID"""

  private val AnnotationsTable: String = s"""/tables/$TABLE_ID/annotations"""

  private val AnnotationCount: String = s"""/tables/annotationCount"""

  private val TranslationStatus: String = s"""/tables/translationStatus"""

  private val CellHistory: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/history"""
  private val CellHistoryWithLangtag: String =
    s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/history/$langtagRegex"""
  private val RowHistory: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/history"""
  private val RowHistoryWithLangtag: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/history/$langtagRegex"""
  private val TableHistory: String = s"""/tables/$TABLE_ID/history"""
  private val TableHistoryWithLangtag: String = s"""/tables/$TABLE_ID/history/$langtagRegex"""

  def route: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    router.getWithRegex(Rows).handler(retrieveRows)
    router.getWithRegex(RowsOfLinkCell).handler(retrieveRowsOfLinkCell)
    router.getWithRegex(RowsOfColumn).handler(retrieveRowsOfColumn)
    router.getWithRegex(RowsOfFirstColumn).handler(retrieveRowsOfFirstColumn)
    router.getWithRegex(Row).handler(retrieveRow)
    router.getWithRegex(RowDependent).handler(retrieveDependentRows)
    router.getWithRegex(Cell).handler(retrieveCell)
    router.getWithRegex(CompleteTableId).handler(retrieveCompleteTable)
    router.getWithRegex(AnnotationsTable).handler(retrieveAnnotations)
    router.getWithRegex(AnnotationCount).handler(retrieveAnnotationCount)
    router.getWithRegex(TranslationStatus).handler(retrieveTranslationStatus)
    router.getWithRegex(ColumnsValues).handler(retrieveUniqueColumnValues)
    router.getWithRegex(ColumnsValuesWithLangtag).handler(retrieveUniqueColumnValuesWithLangtag)

    router.getWithRegex(CellHistory).handler(retrieveCellHistory)
    router.getWithRegex(CellHistoryWithLangtag).handler(retrieveCellHistoryWithLangtag)
    router.getWithRegex(RowHistory).handler(retrieveRowHistory)
    router.getWithRegex(RowHistoryWithLangtag).handler(retrieveRowHistoryWithLangtag)
    router.getWithRegex(TableHistory).handler(retrieveTableHistory)
    router.getWithRegex(TableHistoryWithLangtag).handler(retrieveTableHistoryWithLangtag)

    // DELETE
    router.deleteWithRegex(CellAnnotation).handler(deleteCellAnnotation)
    router.deleteWithRegex(CellAnnotationLangtag).handler(deleteCellAnnotationLangtag)
    router.deleteWithRegex(Row).handler(deleteRow)
    router.deleteWithRegex(Cell).handler(clearCell)
    router.deleteWithRegex(AttachmentOfCell).handler(deleteAttachmentOfCell)
    router.deleteWithRegex(LinkOfCell).handler(deleteLinkOfCell)

    val bodyHandler = BodyHandler.create()
    router.post("/tables/*").handler(bodyHandler)
    router.patch("/tables/*").handler(bodyHandler)
    router.put("/tables/*").handler(bodyHandler)
    router.post("/completetable").handler(bodyHandler)

    // CREATE
    router.postWithRegex(CompleteTable).handler(createCompleteTable)
    router.postWithRegex(Rows).handler(createRow)
    router.postWithRegex(RowDuplicate).handler(duplicateRow)
    router.postWithRegex(CellAnnotations).handler(createCellAnnotation)

    // UPDATE
    router.patchWithRegex(RowAnnotations).handler(updateRowAnnotations)
    router.patchWithRegex(RowsAnnotations).handler(updateRowsAnnotations)

    router.patchWithRegex(Cell).handler(updateCell)
    router.postWithRegex(Cell).handler(updateCell)
    router.putWithRegex(Cell).handler(replaceCell)
    router.putWithRegex(LinkOrderOfCell).handler(changeLinkOrder)

    router
  }

  /**
    * Get Rows
    */
  private def retrieveRows(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)

          val pagination = Pagination(offset, limit)

          controller.retrieveRows(tableId, pagination)
        }
      )
    }
  }

  /**
    * Get foreign rows from a link cell point of view
    * e.g. cardinality in both direction will be considered
    */
  private def retrieveRowsOfLinkCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)
          val pagination = Pagination(offset, limit)
          controller.retrieveForeignRows(tableId, columnId, rowId, pagination)
        }
      )
    }
  }

  /**
    * Get rows of column
    */
  private def retrieveRowsOfColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)

          val pagination = Pagination(offset, limit)

          controller.retrieveRowsOfColumn(tableId, columnId, pagination)
        }
      )
    }
  }

  /**
    * Get rows of first column
    */
  private def retrieveRowsOfFirstColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)

          val pagination = Pagination(offset, limit)

          controller.retrieveRowsOfFirstColumn(tableId, pagination)
        }
      )
    }
  }

  /**
    * Get Row
    */
  private def retrieveRow(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveRow(tableId, rowId)
        }
      )
    }
  }

  /**
    * Get dependent rows
    */
  private def retrieveDependentRows(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveDependentRows(tableId, rowId)
        }
      )
    }
  }

  /**
    * Get Cell
    */
  private def retrieveCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveCell(tableId, columnId, rowId)
        }
      )
    }
  }

  /**
    * Get complete table
    */
  private def retrieveCompleteTable(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveCompleteTable(tableId)
        }
      )
    }
  }

  /**
    * Retrieve all Cell Annotations for a specific table
    */
  private def retrieveAnnotations(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveTableWithCellAnnotations(tableId)
        }
      )
    }
  }

  /**
    * Retrieve Cell Annotation count for all tables
    */
  private def retrieveAnnotationCount(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        controller.retrieveTablesWithCellAnnotationCount()
      }
    )
  }

  /**
    * Retrieve translation status for all tables
    */
  private def retrieveTranslationStatus(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        controller.retrieveTranslationStatus()
      }
    )
  }

  /**
    * Retrieve unique values of a shorttext column
    */
  private def retrieveUniqueColumnValues(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveColumnValues(tableId, columnId, None)
        }
      )
    }
  }

  /**
    *  Retrieve unique values of a multi-language shorttext column
    */
  private def retrieveUniqueColumnValuesWithLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveColumnValues(tableId, columnId, Some(langtag))
        }
      )
    }
  }

  /**
    * Retrieve Cell History
    */
  private def retrieveCellHistory(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      typeOpt = getStringParam("historyType", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveCellHistory(tableId, columnId, rowId, None, typeOpt)
        }
      )
    }
  }

  /**
    * Retrieve Cell History with langtag
    */
  private def retrieveCellHistoryWithLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      langtag <- getLangtag(context)
      typeOpt = getStringParam("historyType", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveCellHistory(tableId.toLong, columnId.toLong, rowId.toLong, Some(langtag), typeOpt)
        }
      )
    }
  }

  /**
    * Retrieve Row History
    */
  private def retrieveRowHistory(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
      typeOpt = getStringParam("historyType", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveRowHistory(tableId.toLong, rowId.toLong, None, typeOpt)
        }
      )
    }
  }

  /**
    * Retrieve Row History with langtag
    */
  private def retrieveRowHistoryWithLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
      langtag <- getLangtag(context)
      typeOpt = getStringParam("historyType", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveRowHistory(tableId.toLong, rowId.toLong, Some(langtag), typeOpt)
        }
      )
    }
  }

  /**
    * Retrieve Table History
    */
  private def retrieveTableHistory(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      typeOpt = getStringParam("historyType", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveTableHistory(tableId.toLong, None, typeOpt)
        }
      )
    }
  }

  /**
    * Retrieve Table History with langtag
    */
  private def retrieveTableHistoryWithLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      langtag <- getLangtag(context)
      typeOpt = getStringParam("historyType", context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveTableHistory(tableId.toLong, Some(langtag), typeOpt)
        }
      )
    }
  }

  /**
    * Create table with columns and rows
    */
  private def createCompleteTable(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)
        for {
          completeTable <- if (json.containsKey("rows")) {
            controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), toRowValueSeq(json))
          } else {
            controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), Seq())
          }
        } yield completeTable
      }
    )
  }

  /**
    * Create Row
    */
  private def createRow(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val optionalValues = Try({
            val json = getJson(context)
            if (json.containsKey("columns") && json.containsKey("rows")) {
              Some(toColumnValueSeq(json))
            } else {
              None
            }
          }).recover({
              case _: NoJsonFoundException => None
            })
            .get

          controller.createRow(tableId, optionalValues)
        }
      )
    }
  }

  /**
    * Duplicate Row
    */
  private def duplicateRow(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.duplicateRow(tableId, rowId)
        }
      )
    }
  }

  /**
    * Update Row Annotations
    */
  private def updateRowAnnotations(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val finalFlagOpt = booleanToValueOption(json.containsKey("final"), json.getBoolean("final", false))
            .map(_.booleanValue())
          for {
            updated <- controller.updateRowAnnotations(tableId, rowId, finalFlagOpt)
          } yield updated
        }
      )
    }
  }

  /**
    * Update all Row Annotations of a table
    */
  private def updateRowsAnnotations(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val finalFlagOpt = booleanToValueOption(json.containsKey("final"), json.getBoolean("final", false))
            .map(_.booleanValue())
          for {
            updated <- controller.updateRowsAnnotations(tableId.toLong, finalFlagOpt)
          } yield updated
        }
      )
    }
  }

  /**
    * Add Cell Annotation (will possibly be merged with an existing annotation)
    */
  private def createCellAnnotation(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          import com.campudus.tableaux.ArgumentChecker._
          val json = getJson(context)
          val langtags = checked(asCastedList[String](json.getJsonArray("langtags", new JsonArray())))
          val flagType = checked(hasString("type", json).map(CellAnnotationType(_)))
          val value = json.getString("value")
          for {
            cellAnnotation <- controller.addCellAnnotation(tableId, columnId, rowId, langtags, flagType, value)
          } yield cellAnnotation
        }
      )
    }
  }

  /**
    * Update Cell or add Link/Attachment
    */
  private def updateCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          for {
            updated <- controller.updateCellValue(tableId, columnId, rowId, json.getValue("value"))
          } yield updated
        }
      )
    }
  }

  /**
    * Replace Cell value
    */
  private def replaceCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          for {
            updated <- if (json.containsKey("value")) {
              controller.replaceCellValue(tableId, columnId, rowId, json.getValue("value"))
            } else {
              Future.failed(InvalidJsonException("request must contain a value", "value_is_missing"))
            }
          } yield updated
        }
      )
    }
  }

  /**
    * Change order of link
    */
  private def changeLinkOrder(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      linkId <- getLinkId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          for {
            updated <- controller.updateCellLinkOrder(tableId, columnId, rowId, linkId, toLocationType(json))
          } yield updated
        }
      )
    }
  }

  /**
    * Delete Cell Annotation
    */
  private def deleteCellAnnotation(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      uuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteCellAnnotation(tableId, columnId, rowId, UUID.fromString(uuid))
        }
      )
    }
  }

  /**
    * Delete Langtag from Cell Annotation
    */
  private def deleteCellAnnotationLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      uuid <- getUUID(context)
      langtag <- getLangtag(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteCellAnnotation(tableId, columnId, rowId, UUID.fromString(uuid), langtag)
        }
      )
    }
  }

  /**
    * Delete Row
    */
  private def deleteRow(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteRow(tableId, rowId)
        }
      )
    }
  }

  /**
    * Clear Cell value
    */
  private def clearCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.clearCellValue(tableId, columnId, rowId)
        }
      )
    }
  }

  /**
    * Delete Attachment from Cell
    */
  private def deleteAttachmentOfCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      uuid <- getUUID(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteAttachment(tableId.toLong, columnId.toLong, rowId.toLong, uuid)
        }
      )
    }
  }

  /**
    * Delete Link from Cell
    */
  private def deleteLinkOfCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      linkId <- getLinkId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteLink(tableId, columnId, rowId, linkId)
        }
      )
    }
  }

  private def getRowId(context: RoutingContext): Option[Long] = {
    getLongParam("rowId", context)
  }

  private def getLinkId(context: RoutingContext): Option[Long] = {
    getLongParam("linkId", context)
  }
}
