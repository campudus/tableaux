package com.campudus.tableaux.router

import java.util.UUID

import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.domain.{CellAnnotationType, Pagination}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{InvalidJsonException, NoJsonFoundException, TableauxConfig}
import org.vertx.scala.core.json.JsonArray
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.concurrent.Future

object TableauxRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => TableauxController): TableauxRouter = {
    new TableauxRouter(config, controllerCurry(config))
  }
}

class TableauxRouter(override val config: TableauxConfig, val controller: TableauxController) extends BaseRouter {

  private val AttachmentOfCell: String =
    s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/attachment/($uuidRegex)"""
  private val LinkOfCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/link/$LINK_ID"""
  private val LinkOrderOfCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/link/$LINK_ID/order"""

  private val ColumnsValues: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/values"""
  private val ColumnsValuesWithLangtag: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/values/($langtagRegex)"""

  private val Cell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID"""
  private val CellAnnotations: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/annotations"""
  private val CellAnnotation: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/annotations/($uuidRegex)"""
  private val CellAnnotationLangtag: String =
    s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/annotations/($uuidRegex)/($langtagRegex)"""

  private val Row: String = s"""/tables/$TABLE_ID/rows/$ROW_ID"""
  private val RowDuplicate: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/duplicate"""
  private val RowDependent: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/dependent"""
  private val RowAnnotations: String = s"""/tables/$TABLE_ID/rows/$ROW_ID/annotations"""
  private val Rows: String = s"""/tables/$TABLE_ID/rows"""
  private val RowsFOO: String = s"""/tables/:id/rows"""
  private val RowsAnnotations: String = s"""/tables/$TABLE_ID/rows/annotations"""
  private val RowsOfColumn: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows"""
  private val RowsOfFirstColumn: String = s"""/tables/$TABLE_ID/columns/first/rows"""
  private val RowsOfLinkCell: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID/rows/$ROW_ID/foreignRows"""

  private val CompleteTable: String = s"""/completetable"""
  private val CompleteTableId: String = s"""/completetable/$TABLE_ID"""

  private val AnnotationsTable: String = s"""/tables/$TABLE_ID/annotations"""

  private val AnnotationCount: String = s"""/tables/annotationCount"""

  private val TranslationStatus: String = s"""/tables/translationStatus"""

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

    // DELETE
    router.deleteWithRegex(CellAnnotation).handler(deleteCellAnnotation)
    router.deleteWithRegex(CellAnnotationLangtag).handler(deleteCellAnnotationLangtag)
    router.deleteWithRegex(Row).handler(deleteRow)
    router.deleteWithRegex(Cell).handler(clearCell)
    router.deleteWithRegex(AttachmentOfCell).handler(deleteAttachmentOfCell)
    router.deleteWithRegex(LinkOfCell).handler(deleteLinkOfCell)

    router
  }

  /**
    * Get Rows
    */
  def retrieveRows(context: RoutingContext): Unit = {
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
  def retrieveRowsOfLinkCell(context: RoutingContext): Unit = {
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
  def retrieveRowsOfColumn(context: RoutingContext): Unit = {
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
  def retrieveRowsOfFirstColumn(context: RoutingContext): Unit = {
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
  def retrieveRow(context: RoutingContext): Unit = {
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
  def retrieveDependentRows(context: RoutingContext): Unit = {
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
  def retrieveCell(context: RoutingContext): Unit = {
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
  def retrieveCompleteTable(context: RoutingContext): Unit = {
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
    * Create table with columns and rows
    */
  def createCompleteTable(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        for {
          json <- getJson(context)
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
  def createRow(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            optionalValues <- (for {
              json <- getJson(context)
              option = if (json.containsKey("columns") && json.containsKey("rows")) {
                Some(toColumnValueSeq(json))
              } else {
                None
              }
            } yield option) recover {
              case _: NoJsonFoundException => None
            }
            result <- controller.createRow(tableId, optionalValues)
          } yield result
        }
      )
    }
  }

  /**
    * Duplicate Row
    */
  def duplicateRow(context: RoutingContext): Unit = {
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
  def updateRowAnnotations(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            finalFlagOpt = booleanToValueOption(json.containsKey("final"), json.getBoolean("final", false))
              .map(_.booleanValue())

            updated <- controller.updateRowAnnotations(tableId, rowId, finalFlagOpt)
          } yield updated
        }
      )
    }
  }

  /**
    * Update all Row Annotations of a table
    */
  def updateRowsAnnotations(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            finalFlagOpt = booleanToValueOption(json.containsKey("final"), json.getBoolean("final", false))
              .map(_.booleanValue())

            updated <- controller.updateRowsAnnotations(tableId.toLong, finalFlagOpt)
          } yield updated
        }
      )
    }
  }

  /**
    * Add Cell Annotation (will possibly be merged with an existing annotation)
    */
  def createCellAnnotation(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          import com.campudus.tableaux.ArgumentChecker._

          for {
            json <- getJson(context)

            langtags = checked(asCastedList[String](json.getJsonArray("langtags", new JsonArray())))
            flagType = checked(hasString("type", json).map(CellAnnotationType(_)))
            value = json.getString("value")

            cellAnnotation <- controller.addCellAnnotation(tableId, columnId, rowId, langtags, flagType, value)
          } yield cellAnnotation
        }
      )
    }
  }

  /**
    * Delete Cell Annotation
    */
  def deleteCellAnnotation(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      //TODO get uuid
      uuid = "todo"
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
  def deleteCellAnnotationLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      //TODO get uuid
      uuid = "todo"
      //TODO get langtag
      langtag = "todo"
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
    * Retrieve all Cell Annotations for a specific table
    */
  def retrieveAnnotations(context: RoutingContext): Unit = {
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
  def retrieveAnnotationCount(context: RoutingContext): Unit = {
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
  def retrieveTranslationStatus(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        controller.retrieveTranslationStatus()
      }
    )
  }

  /**
    * Update Cell or add Link/Attachment
    */
  def updateCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            updated <- controller.updateCellValue(tableId, columnId, rowId, json.getValue("value"))
          } yield updated
        }
      )
    }
  }

  /**
    * Replace Cell value
    */
  def replaceCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
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
  def changeLinkOrder(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      linkId <- getLinkId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            updated <- controller.updateCellLinkOrder(tableId, columnId, rowId, linkId, toLocationType(json))
          } yield updated
        }
      )
    }
  }

  /**
    * Delete Row
    */
  def deleteRow(context: RoutingContext): Unit = {
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
  def clearCell(context: RoutingContext): Unit = {
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
  def deleteAttachmentOfCell(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
      //TODO get uuid
      uuid = "todo"
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
  def deleteLinkOfCell(context: RoutingContext): Unit = {
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

  /**
    * Retrieve unique values of a shorttext column
    */
  def retrieveUniqueColumnValues(context: RoutingContext): Unit = {
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
  def retrieveUniqueColumnValuesWithLangtag(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      //TODO get langtag
      langtag = "todo"
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveColumnValues(tableId, columnId, Some(langtag))
        }
      )
    }
  }
}
