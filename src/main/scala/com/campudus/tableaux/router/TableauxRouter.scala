package com.campudus.tableaux.router

import com.campudus.tableaux.{InvalidJsonException, NoJsonFoundException, TableauxConfig}
import com.campudus.tableaux.OkArg
import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database.domain.{CellAnnotationType, Pagination}
import com.campudus.tableaux.database.model.DuplicateRowOptions
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.router.auth.permission.TableauxUser

import io.vertx.scala.ext.web.{Router, RoutingContext}
import io.vertx.scala.ext.web.handler.BodyHandler
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.Try

import java.util.UUID

object TableauxRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => TableauxController): TableauxRouter = {
    new TableauxRouter(config, controllerCurry(config))
  }
}

class TableauxRouter(override val config: TableauxConfig, val controller: TableauxController) extends BaseRouter {

  private val attachmentOfCell: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/attachment/$uuidRegex"
  private val linkOfCell: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/link/$linkId"
  private val linkOrderOfCell: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/link/$linkId/order"

  private val columnsValues: String = s"/tables/$tableId/columns/$columnId/values"
  private val columnsValuesWithLangtag: String = s"/tables/$tableId/columns/$columnId/values/$langtagRegex"

  private val cell: String = s"/tables/$tableId/columns/$columnId/rows/$rowId"
  private val cellAnnotations: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/annotations"
  private val cellAnnotation: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/annotations/$uuidRegex"

  private val cellAnnotationLangtag: String =
    s"/tables/$tableId/columns/$columnId/rows/$rowId/annotations/$uuidRegex/$langtagRegex"

  private val row: String = s"/tables/$tableId/rows/$rowId"
  private val rowDuplicate: String = s"/tables/$tableId/rows/$rowId/duplicate"
  private val rowDependent: String = s"/tables/$tableId/rows/$rowId/dependent"
  private val rowAnnotations: String = s"/tables/$tableId/rows/$rowId/annotations"
  private val rows: String = s"/tables/$tableId/rows"
  private val rowsAnnotations: String = s"/tables/$tableId/rows/annotations"
  private val rowsOfColumn: String = s"/tables/$tableId/columns/$columnId/rows"
  private val rowsOfFirstColumn: String = s"/tables/$tableId/columns/first/rows"
  private val rowsOfLinkCell: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/foreignRows"

  private val completeTable: String = s"/completetable"
  private val completeTableId: String = s"/completetable/$tableId"

  private val annotationsTable: String = s"/tables/$tableId/annotations"
  private val annotationCount: String = s"/tables/annotationCount"

  private val translationStatus: String = s"/tables/translationStatus"

  private val cellHistory: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/history"
  private val cellHistoryWithLangtag: String = s"/tables/$tableId/columns/$columnId/rows/$rowId/history/$langtagRegex"
  private val rowHistory: String = s"/tables/$tableId/rows/$rowId/history"
  private val rowHistoryWithLangtag: String = s"/tables/$tableId/rows/$rowId/history/$langtagRegex"
  private val tableHistory: String = s"/tables/$tableId/history"
  private val tableHistoryWithLangtag: String = s"/tables/$tableId/history/$langtagRegex"

  private val rowPermissionsPath: String = s"/tables/$tableId/rows/$rowId/permissions"

  def route: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    router.getWithRegex(rows).handler(retrieveRows)
    router.getWithRegex(rowsOfLinkCell).handler(retrieveRowsOfLinkCell)
    router.getWithRegex(rowsOfColumn).handler(retrieveRowsOfColumn)
    router.getWithRegex(rowsOfFirstColumn).handler(retrieveRowsOfFirstColumn)
    router.getWithRegex(row).handler(retrieveRow)
    router.getWithRegex(rowDependent).handler(retrieveDependentRows)
    router.getWithRegex(cell).handler(retrieveCell)
    router.getWithRegex(completeTableId).handler(retrieveCompleteTable)
    router.getWithRegex(annotationsTable).handler(retrieveAnnotations)
    router.getWithRegex(annotationCount).handler(retrieveAnnotationCount)
    router.getWithRegex(translationStatus).handler(retrieveTranslationStatus)
    router.getWithRegex(columnsValues).handler(retrieveUniqueColumnValues)
    router.getWithRegex(columnsValuesWithLangtag).handler(retrieveUniqueColumnValuesWithLangtag)

    router.getWithRegex(cellHistory).handler(retrieveCellHistory)
    router.getWithRegex(cellHistoryWithLangtag).handler(retrieveCellHistoryWithLangtag)
    router.getWithRegex(rowHistory).handler(retrieveRowHistory)
    router.getWithRegex(rowHistoryWithLangtag).handler(retrieveRowHistoryWithLangtag)
    router.getWithRegex(tableHistory).handler(retrieveTableHistory)
    router.getWithRegex(tableHistoryWithLangtag).handler(retrieveTableHistoryWithLangtag)

    router.getWithRegex(cellAnnotations).handler(retrieveCellAnnotations)

    // DELETE
    router.deleteWithRegex(cellAnnotation).handler(deleteCellAnnotation)
    router.deleteWithRegex(cellAnnotationLangtag).handler(deleteCellAnnotationLangtag)
    router.deleteWithRegex(row).handler(deleteRow)
    router.deleteWithRegex(cell).handler(clearCell)
    router.deleteWithRegex(attachmentOfCell).handler(deleteAttachmentOfCell)
    router.deleteWithRegex(linkOfCell).handler(deleteLinkOfCell)

    val bodyHandler = BodyHandler.create()
    router.post("/tables/*").handler(bodyHandler)
    router.patch("/tables/*").handler(bodyHandler)
    router.put("/tables/*").handler(bodyHandler)
    router.post("/completetable").handler(bodyHandler)

    // CREATE
    router.postWithRegex(completeTable).handler(createCompleteTable)
    router.postWithRegex(rows).handler(createRow)
    router.postWithRegex(rowDuplicate).handler(duplicateRow)
    router.postWithRegex(cellAnnotations).handler(createCellAnnotation)

    // UPDATE
    router.patchWithRegex(rowAnnotations).handler(updateRowAnnotations)
    router.patchWithRegex(rowsAnnotations).handler(updateRowsAnnotations)

    router.patchWithRegex(cell).handler(updateCell)
    router.postWithRegex(cell).handler(updateCell)
    router.putWithRegex(cell).handler(replaceCell)
    router.putWithRegex(linkOrderOfCell).handler(changeLinkOrder)

    router.patchWithRegex(rowPermissionsPath).handler(addRowPermissions)
    router.deleteWithRegex(rowPermissionsPath).handler(deleteRowPermissions)
    router.putWithRegex(rowPermissionsPath).handler(replaceRowPermissions)

    router
  }

  /**
    * Get rows
    */
  private def retrieveRows(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)
          val finalFlagOpt = getBoolParam("final", context)
          val archivedFlagOpt = getBoolParam("archived", context)
          val pagination = Pagination(offset, limit)

          controller.retrieveRows(tableId, finalFlagOpt, archivedFlagOpt, pagination)
        }
      )
    }
  }

  /**
    * Get foreign rows from a link cell point of view e.g. cardinality in both direction will be considered
    */
  private def retrieveRowsOfLinkCell(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val finalFlagOpt = getBoolParam("final", context)
          val archivedFlagOpt = getBoolParam("archived", context)
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)

          val pagination = Pagination(offset, limit)

          controller.retrieveRowsOfColumn(tableId, columnId, finalFlagOpt, archivedFlagOpt, pagination)
        }
      )
    }
  }

  /**
    * Get rows of first column
    */
  private def retrieveRowsOfFirstColumn(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val limit = getLongParam("limit", context)
          val offset = getLongParam("offset", context)
          val finalFlagOpt = getBoolParam("final", context)
          val archivedFlagOpt = getBoolParam("archived", context)
          val pagination = Pagination(offset, limit)

          controller.retrieveRowsOfFirstColumn(tableId, finalFlagOpt, archivedFlagOpt, pagination)
        }
      )
    }
  }

  /**
    * Get row
    */
  private def retrieveRow(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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

  private def retrieveCellAnnotations(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveCellAnnotations(tableId, columnId, rowId)
        }
      )
    }
  }

  /**
    * Get complete table
    */
  private def retrieveCompleteTable(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    * Retrieve unique values of a multi-language shorttext column
    */
  private def retrieveUniqueColumnValuesWithLangtag(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    * Retrieve row History
    */
  private def retrieveRowHistory(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    * Retrieve row History with langtag
    */
  private def retrieveRowHistoryWithLangtag(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)
        for {
          completeTable <-
            if (json.containsKey("rows")) {
              controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), toRowValueSeq(json))
            } else {
              controller.createCompleteTable(json.getString("name"), toCreateColumnSeq(json), Seq())
            }
        } yield completeTable
      }
    )
  }

  /**
    * Create row
    */
  private def createRow(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    def getOptionalValues(json: JsonObject) = {
      if (json.containsKey("columns") && json.containsKey("rows")) {
        Some(toColumnValueSeq(json))
      } else {
        None
      }
    }

    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = Try(getJson(context)).getOrElse(Json.emptyObj())
          val optionalValues = Try(getOptionalValues(json))
            .recover({
              case _: NoJsonFoundException => None
            })
            .get
          val rowPermissionsOpt = getRowPermissionsOpt("rowPermissions", json)
          controller.createRow(tableId, optionalValues, rowPermissionsOpt)
        }
      )
    }
  }

  /**
    * Duplicate row
    */
  private def duplicateRow(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    val body = Try(getJson(context)).toOption
    val specificColumnsIds =
      body.filter(_.containsKey("columns"))
        .map(toColumnIdSeq)
        .flatMap(_.toOption)
        .flatMap(_ match {
          case OkArg(value) => Some(value)
          case _ => None
        })
    val duplicateOptions = DuplicateRowOptions(
      getBoolParam("skipConstrainedLinks", context).getOrElse(false),
      getBoolParam("annotateSkipped", context).getOrElse(false),
      specificColumnsIds
    )
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.duplicateRow(tableId, rowId, Some(duplicateOptions))
        }
      )
    }
  }

  /**
    * Update row Annotations
    */
  private def updateRowAnnotations(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val finalFlagOpt = getBooleanOption("final", false, json)
          val archivedFlagOpt = getBooleanOption("archived", false, json)
          for {
            updated <- controller.updateRowAnnotations(tableId, rowId, finalFlagOpt, archivedFlagOpt)
          } yield updated
        }
      )
    }
  }

  /**
    * Update all row Annotations of a table
    */
  private def updateRowsAnnotations(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val finalFlagOpt = getBooleanOption("final", false, json)
          val archivedFlagOpt = getBooleanOption("archived", false, json)
          for {
            updated <- controller.updateRowsAnnotations(tableId.toLong, finalFlagOpt, archivedFlagOpt)
          } yield updated
        }
      )
    }
  }

  /**
    * Add Cell Annotation (will possibly be merged with an existing annotation)
    */
  private def createCellAnnotation(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
    val forceHistory = getBoolQuery("forceHistory", context).getOrElse(false)
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
            updated <- controller.updateCellValue(tableId, columnId, rowId, json.getValue("value"), forceHistory)
          } yield updated
        }
      )
    }
  }

  /**
    * Replace Cell value
    */
  private def replaceCell(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    val forceHistory = getBoolQuery("forceHistory", context).getOrElse(false)
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
            updated <-
              if (json.containsKey("value")) {
                controller.replaceCellValue(tableId, columnId, rowId, json.getValue("value"), forceHistory)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    * Delete row
    */
  private def deleteRow(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    val replacingRowIdStringOpt = context.queryParams().get("replacingRowId")
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteRow(tableId, rowId, replacingRowIdStringOpt)
        }
      )
    }
  }

  /**
    * Clear Cell value
    */
  private def clearCell(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    implicit val user = TableauxUser(context)
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
    * Add permissions to Row
    */
  private def addRowPermissions(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val rowPermissions = getRowPermissionsOpt("value", json).getOrElse(Seq())
          controller.addRowPermissions(tableId, rowId, rowPermissions)
        }
      )
    }
  }

  /**
    * Delete all permissions from Row
    */
  private def deleteRowPermissions(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.deleteRowPermissions(tableId, rowId)
        }
      )
    }
  }

  /**
    * Replace permissions of Row
    */
  private def replaceRowPermissions(context: RoutingContext): Unit = {
    implicit val user = TableauxUser(context)
    for {
      tableId <- getTableId(context)
      rowId <- getRowId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val rowPermissions = getRowPermissionsOpt("value", json).getOrElse(Seq())
          controller.replaceRowPermissions(tableId, rowId, rowPermissions)
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
