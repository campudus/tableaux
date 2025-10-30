package com.campudus.tableaux.controller

import com.campudus.tableaux.{TableauxConfig, UnprocessableEntityException}
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.ForbiddenException
import com.campudus.tableaux.database.{LanguageNeutral, LocationType}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.domain.DisplayInfos.Langtag
import com.campudus.tableaux.database.model.{Attachment, TableauxModel}
import com.campudus.tableaux.database.model.DuplicateRowOptions
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.verticles.EventClient

import org.vertx.scala.core.json.Json

import scala.concurrent.Future
import scala.util.Try

import java.util.UUID

object TableauxController {

  def apply(config: TableauxConfig, repository: TableauxModel, roleModel: RoleModel): TableauxController = {
    new TableauxController(config, repository, roleModel)
  }
}

class TableauxController(
    override val config: TableauxConfig,
    override protected val repository: TableauxModel,
    implicit protected val roleModel: RoleModel
) extends Controller[TableauxModel] {

  val eventClient: EventClient = EventClient(vertx)

  def addCellAnnotation(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  )(implicit user: TableauxUser): Future[CellLevelAnnotation] = {
    logger.info(s"addCellAnnotation $tableId $columnId $rowId $langtags $annotationType $value")
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ = if (column.languageType == LanguageNeutral && langtags.nonEmpty) {
        throw UnprocessableEntityException(
          s"Cannot add an annotation with langtags to a language neutral cell (table: $tableId, column: $columnId)"
        )
      }
      _ = if (annotationType == FlagAnnotationType && value == "needs_translation" && langtags.isEmpty) {
        throw UnprocessableEntityException(s"Cannot add/change a 'needs_translation' annotation without langtags")
      }
      _ <- roleModel.checkAuthorization(EditCellAnnotation, ComparisonObjects(table))

      cellAnnotation <- repository.addCellAnnotation(column, rowId, langtags, annotationType, value)
    } yield {
      eventClient.cellAnnotationChanged(tableId, columnId, rowId)
      cellAnnotation
    }
  }

  def deleteCellAnnotation(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: UUID)(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"deleteCellAnnotation $tableId $columnId $rowId $uuid")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(EditCellAnnotation, ComparisonObjects(table))
      _ <- repository.deleteCellAnnotation(column, rowId, uuid)
    } yield {
      eventClient.cellAnnotationChanged(tableId, columnId, rowId)
      EmptyObject()
    }
  }

  def deleteCellAnnotation(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      uuid: UUID,
      langtag: String
  )(implicit user: TableauxUser): Future[EmptyObject] = {
    checkArguments(
      greaterZero(tableId),
      greaterThan(columnId, -1, "columnId"),
      greaterZero(rowId),
      notNull(langtag, "langtag")
    )
    logger.info(s"deleteCellAnnotation $tableId $columnId $rowId $uuid $langtag")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ = if (column.languageType == LanguageNeutral) {
        throw UnprocessableEntityException(
          s"There are no annotations with langtags on a language neutral cell (table: $tableId, column: $columnId)"
        )
      }
      _ <- roleModel.checkAuthorization(EditCellAnnotation, ComparisonObjects(table))

      _ <- repository.deleteCellAnnotation(column, rowId, uuid, langtag)
    } yield EmptyObject()
  }

  def retrieveTableWithCellAnnotations(tableId: TableId)(
      implicit user: TableauxUser
  ): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTableWithCellAnnotations $tableId")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(ViewTable, ComparisonObjects(table))

      annotations <- repository.retrieveTableWithCellAnnotations(table)
    } yield annotations
  }

  def retrieveTablesWithCellAnnotationCount()(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info(s"retrieveTablesWithCellAnnotationCount")

    for {
      tables <- repository.retrieveTables()

      tablesWithCellAnnotationCount <-
        if (tables.isEmpty) {
          Future.successful(Seq.empty[TableWithCellAnnotationCount])
        } else {
          repository.retrieveTablesWithCellAnnotationCount(tables)
        }
    } yield {
      PlainDomainObject(Json.obj("tables" -> tablesWithCellAnnotationCount.map(_.getJson)))
    }
  }

  def retrieveTranslationStatus()(implicit user: TableauxUser): Future[DomainObject] = {
    logger.info(s"retrieveTranslationStatus")

    for {
      tables <- repository.retrieveTables(isInternalCall = true)

      tablesWithMultiLanguageColumnCount <- Future.sequence(
        tables.map(table =>
          repository
            .retrieveColumns(table, isInternalCall = true)
            .map(columns => {
              val multiLanguageColumnsCount = columns.count({
                case MultiLanguageColumn(_) => true
                case _ => false
              })

              (table, multiLanguageColumnsCount)
            })
        )
      )

      relevantTables = tablesWithMultiLanguageColumnCount
        .filter({
          case (_, count) if count > 0 => true
          case _ => false
        })
        .map({ case (table, _) => table })

      tablesForWhichViewIsGranted = roleModel
        .filterDomainObjects[Table](ViewTable, relevantTables, isInternalCall = false)

      tablesWithCellAnnotationCount <- repository.retrieveTablesWithCellAnnotationCount(relevantTables)
    } yield {
      val tablesWithMultiLanguageColumnCountMap = tablesWithMultiLanguageColumnCount.toMap

      val translationStatusByTable = tablesWithCellAnnotationCount.map({
        case TableWithCellAnnotationCount(table, totalSize, annotationCount) =>
          val langtags = table.langtags.getOrElse(Seq.empty)

          val multiLanguageColumnsCount = tablesWithMultiLanguageColumnCountMap.getOrElse(table, 0)

          val multiLanguageCellCount = totalSize * multiLanguageColumnsCount

          val needsTranslationCount = annotationCount.filter({
            case CellAnnotationCount(FlagAnnotationType, Some("needs_translation"), _, _, _) => true
            case _ => false
          })

          val needsTranslationStatusByLangtag = langtags
            .map(langtag => {
              val count = needsTranslationCount
                .map({
                  case CellAnnotationCount(_, _, Some(`langtag`), count: Long, _) => count
                  case _ => 0
                })
                .sum

              val percentage =
                if (count > 0 && multiLanguageCellCount > 0) {
                  1.0 - (count.toDouble / multiLanguageCellCount.toDouble)
                } else {
                  1.0
                }

              (langtag, percentage)
            })

          (table, multiLanguageColumnsCount, totalSize, needsTranslationStatusByLangtag)
      })

      val translationStatusByTableJson = translationStatusByTable
        .filter({
          case (table, _, _, _) if tablesForWhichViewIsGranted.contains(table) => true
          case _ => false
        })
        .map({
          case (table, _, _, needsTranslationStatusForLangtags) =>
            table.getJson.mergeIn(
              Json.obj(
                "translationStatus" -> Json.obj(needsTranslationStatusForLangtags: _*)
              )
            )
        })

      val mergedLangtags = tables.foldLeft(Seq.empty[String])({
        case (langtagsAcc, table) =>
          (langtagsAcc ++ table.langtags.getOrElse(Seq.empty)).distinct
      })

      val translationStatus = mergedLangtags.map({ langtag =>
        val mergedTranslationStatusForLangtag = translationStatusByTable
          .flatMap({
            case (_, _, _, translationStatusByLangtag) => translationStatusByLangtag
          })
          .filter({
            case (`langtag`, _) => true
            case _ => false
          })
          .map(_._2)

        val calculatedLangtagStatus = mergedTranslationStatusForLangtag.size match {
          case 0 => 1
          case _ => mergedTranslationStatusForLangtag.sum / mergedTranslationStatusForLangtag.size
        }

        (langtag, calculatedLangtagStatus)
      })

      PlainDomainObject(
        Json.obj(
          "tables" -> translationStatusByTableJson,
          "translationStatus" -> Json.obj(translationStatus: _*)
        )
      )
    }
  }

  def createRow(
      tableId: TableId,
      values: Option[Seq[Seq[(ColumnId, _)]]],
      rowPermissionsOpt: Option[Seq[String]] = None
  )(
      implicit user: TableauxUser
  ): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))

    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(CreateRow, ComparisonObjects(table))
      row <- values match {
        case Some(seq) =>
          checkArguments(nonEmpty(seq, "rows"))
          logger.info(s"createRows ${table.id} $values")
          for {
            createdRows <- repository.createRows(table, seq, rowPermissionsOpt)
          } yield {
            createdRows.rows.foreach(row => eventClient.rowCreated(tableId, row.id))
            createdRows
          }
        case None =>
          logger.info(s"createRow ${table.id}")
          for {
            createdRow <- repository.createRow(table, rowPermissionsOpt)
          } yield {
            eventClient.rowCreated(tableId, createdRow.id)
            createdRow
          }
      }
    } yield row
  }

  def updateRow(
      tableId: TableId,
      rowId: RowId,
      values: Seq[(ColumnId, _)],
      rowPermissionsObj: Option[Seq[String]] = None
  )(
      implicit user: TableauxUser
  ): Future[DomainObject] = {
    logger.info(s"updateRow table $tableId row $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- repository.updateRow(table, rowId, values, rowPermissionsObj)
      updatedRow <- repository.retrieveRow(table, rowId)
    } yield updatedRow
  }

  def setRow(
      tableId: TableId,
      rowId: RowId,
      values: Seq[(ColumnId, _)],
      rowPermissionsObj: Option[Seq[String]] = None
  )(
      implicit user: TableauxUser
  ): Future[DomainObject] = {
    logger.info(s"setRow table $tableId row $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- repository.setRow(table, rowId, values, rowPermissionsObj)
      updatedRow <- repository.retrieveRow(table, rowId)
    } yield updatedRow
  }

  private def checkForTaxonomyTable(table: Table, archivedFlagOpt: Option[Boolean]): Unit = {
    if (table.tableType == TaxonomyTable && archivedFlagOpt.isDefined) {
      throw ForbiddenException(s"Archived flag is not allowed on taxonomy tables (table: ${table.id})", "archive")
    }
  }

  def updateRowAnnotations(
      tableId: TableId,
      rowId: RowId,
      finalFlagOpt: Option[Boolean],
      archivedFlagOpt: Option[Boolean]
  )(
      implicit user: TableauxUser
  ): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"updateRowAnnotations $tableId $rowId $finalFlagOpt $archivedFlagOpt")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(EditRowAnnotation, ComparisonObjects(table))
      _ = checkForTaxonomyTable(table, archivedFlagOpt)
      updatedRow <- repository.updateRowAnnotations(table, rowId, finalFlagOpt, archivedFlagOpt)
    } yield {
      eventClient.rowAnnotationChanged(tableId, rowId)
      updatedRow
    }
  }

  def updateRowsAnnotations(tableId: TableId, finalFlagOpt: Option[Boolean], archivedFlagOpt: Option[Boolean])(
      implicit user: TableauxUser
  ): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"updateRowsAnnotations $tableId $finalFlagOpt $archivedFlagOpt")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(EditRowAnnotation, ComparisonObjects(table))
      _ = checkForTaxonomyTable(table, archivedFlagOpt)
      _ <- repository.updateRowsAnnotations(table, finalFlagOpt, archivedFlagOpt)
    } yield EmptyObject()
  }

  def duplicateRow(tableId: TableId, rowId: RowId, duplicateOptions: Option[DuplicateRowOptions])(implicit
  user: TableauxUser): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"duplicateRow $tableId $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      duplicated <- repository.duplicateRow(table, rowId, duplicateOptions)
      _ <- eventClient.invalidateRow(table.id, rowId)
    } yield duplicated
  }

  def retrieveRow(tableId: TableId, rowId: RowId)(implicit user: TableauxUser): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveRow $tableId $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      row <- repository.retrieveRow(table, rowId)
    } yield row
  }

  def retrieveRows(
      tableId: TableId,
      finalFlagOpt: Option[Boolean] = None,
      archivedFlagOpt: Option[Boolean] = None,
      pagination: Pagination = Pagination(None, None),
      columnFilter: ColumnType[_] => Boolean = _ => true
  )(
      implicit user: TableauxUser
  ): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), pagination.check)
    logger.info(s"retrieveRows $tableId, finalFlag: $finalFlagOpt, archivedFlag: $archivedFlagOpt")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, finalFlagOpt, archivedFlagOpt, pagination, columnFilter)
    } yield rows
  }

  def retrieveRowsOfFirstColumn(
      tableId: TableId,
      finalFlagOpt: Option[Boolean] = None,
      archivedFlagOpt: Option[Boolean] = None,
      pagination: Pagination = Pagination(None, None)
  )(
      implicit user: TableauxUser
  ): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), pagination.check)
    logger.info(
      s"retrieveRowsOfFirstColumn $tableId for first column, finalFlag: $finalFlagOpt, archivedFlag: $archivedFlagOpt"
    )

    for {
      table <- repository.retrieveTable(tableId)
      columns <- repository.retrieveColumns(table)
      rows <- repository.retrieveRows(table, columns.head.id, finalFlagOpt, archivedFlagOpt, pagination)
    } yield rows
  }

  def retrieveRowsOfColumn(
      tableId: TableId,
      columnId: ColumnId,
      finalFlagOpt: Option[Boolean] = None,
      archivedFlagOpt: Option[Boolean] = None,
      pagination: Pagination = Pagination(None, None)
  )(
      implicit user: TableauxUser
  ): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), pagination.check)
    logger.info(s"retrieveRows $tableId for column $columnId, finalFlag: $finalFlagOpt, archivedFlag: $archivedFlagOpt")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, columnId, finalFlagOpt, archivedFlagOpt, pagination)
    } yield rows
  }

  def retrieveForeignRows(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      finalFlagOpt: Option[Boolean] = None,
      archivedFlagOpt: Option[Boolean] = None,
      pagination: Pagination = Pagination(None, None)
  )(implicit user: TableauxUser): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId), pagination.check)
    logger.info(s"retrieveForeignRows $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveForeignRows(table, columnId, rowId, finalFlagOpt, archivedFlagOpt, pagination)
    } yield rows
  }

  def retrieveCellAnnotations(tableId: TableId, columnId: ColumnId, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[CellLevelAnnotations] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"retrieveCell $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      annotations <- repository.retrieveCellAnnotations(table, columnId, rowId)
    } yield annotations
  }

  def retrieveCell(tableId: TableId, columnId: ColumnId, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"retrieveCell $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      cell <- repository.retrieveCell(table, columnId, rowId)
    } yield cell
  }

  def retrieveDependentRows(tableId: TableId, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[DependentRowsSeq] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveDependentRows $tableId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(ViewTable, ComparisonObjects(table))
      dependentRows <- repository.retrieveDependentRows(table, rowId)
    } yield dependentRows
  }

  def deleteRow(
      tableId: TableId,
      rowId: RowId,
      replacingRowIdStringOpt: Option[String] = None
  )(implicit user: TableauxUser): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    val replacingRowIdOpt = replacingRowIdStringOpt match {
      case None => None
      case Some(idString) => {
        Try(idString.toInt).toOption match {
          case None => throw new UnprocessableEntityException(s"Invalid replacing row id: $idString")
          case Some(id) => {
            checkArguments(greaterZero(id))
            Some(id)
          }
        }
      }
    }
    logger.info(s"deleteRow $tableId $rowId $replacingRowIdOpt")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(DeleteRow, ComparisonObjects(table))
      _ <- repository.deleteRow(table, rowId, replacingRowIdOpt)
    } yield {
      eventClient.rowDeleted(tableId, rowId)
      EmptyObject()
    }
  }

  def deleteLink(tableId: TableId, columnId: ColumnId, rowId: RowId, toId: RowId)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), greaterZero(toId))
    logger.info(s"deleteLink $tableId $columnId $rowId $toId")

    for {
      table <- repository.retrieveTable(tableId)
      updated <- repository.deleteLink(table, columnId, rowId, toId)
    } yield updated
  }

  def updateCellLinkOrder(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      toId: RowId,
      locationType: LocationType
  )(implicit user: TableauxUser): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), greaterZero(toId))
    logger.info(s"updateCellLinkOrder $tableId $columnId $rowId $toId $locationType")
    for {
      table <- repository.retrieveTable(tableId)
      filled <- repository.updateCellLinkOrder(table, columnId, rowId, toId, locationType)
    } yield filled
  }

  def replaceCellValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A, forceHistory: Boolean = false)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"replaceCellValue $tableId $columnId $rowId $value")
    for {
      table <- repository.retrieveTable(tableId)
      filled <- repository.replaceCellValue(table, columnId, rowId, value, forceHistory)
    } yield filled
  }

  def updateCellValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A, forceHistory: Boolean = false)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"updateCellValue $tableId $columnId $rowId $value")

    for {
      table <- repository.retrieveTable(tableId)
      updated <- repository.updateCellValue(table, columnId, rowId, value, forceHistory)
      _ <- eventClient.cellChanged(tableId, columnId, rowId)
    } yield updated
  }

  def clearCellValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"clearCellValue $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      cleared <- repository.clearCellValue(table, columnId, rowId)
    } yield cleared
  }

  def deleteAttachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: String)(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(uuid, "uuid"))
    logger.info(s"deleteAttachment $tableId $columnId $rowId $uuid")

    // TODO introduce a Cell identifier with tableId, columnId and rowId
    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)

      _ <- roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column))

      _ <- repository.createHistoryModel.createCellsInit(table, rowId, Seq((column, uuid)))
      _ <- repository.attachmentModel.delete(Attachment(tableId, columnId, rowId, UUID.fromString(uuid), None))
      _ <- eventClient.invalidateCellValue(tableId, columnId, rowId)
      _ <- repository.createHistoryModel.createCells(table, rowId, Seq((column, uuid)))
    } yield EmptyObject()
  }

  def retrieveCompleteTable(tableId: TableId)(implicit user: TableauxUser): Future[CompleteTable] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveCompleteTable $tableId")

    for {
      table <- repository.retrieveTable(tableId)
      colList <- repository.retrieveColumns(table)
      rowList <- repository.retrieveRows(table, None, None, Pagination(None, None))
    } yield CompleteTable(table, colList, rowList)
  }

  def createCompleteTable(tableName: String, columns: Seq[CreateColumn], rows: Seq[Seq[_]])(
      implicit user: TableauxUser
  ): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $columns rows $rows")

    for {
      _ <- roleModel.checkAuthorization(CreateTable)
      _ <- roleModel.checkAuthorization(CreateColumn)
      _ <-
        if (!rows.headOption.exists(_.isEmpty)) {
          roleModel.checkAuthorization(CreateRow)
        } else {
          Future.successful(())
        }
      table <- repository.createTable(tableName, hidden = false)
      columnIds <- repository.createColumns(table, columns).map(_.map(_.id))
      _ <- repository.createRows(table, rows.map(columnIds.zip(_)), None)
      completeTable <- retrieveCompleteTable(table.id)
    } yield completeTable
  }

  def retrieveColumnValues(tableId: TableId, columnId: ColumnId, langtagOpt: Option[Langtag])(
      implicit user: TableauxUser
  ): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"retrieveColumnValues $tableId $columnId $langtagOpt")

    for {
      table <- repository.retrieveTable(tableId)
      values <- repository.retrieveColumnValues(table, columnId, langtagOpt)
    } yield PlainDomainObject(Json.obj("values" -> values))
  }

  def retrieveCellHistory(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      langtagOpt: Option[Langtag],
      typeOpt: Option[String],
      includeDeleted: Boolean = false
  )(implicit user: TableauxUser): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"retrieveCellHistory $tableId $columnId $rowId $langtagOpt $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveCellHistory(table, columnId, rowId, langtagOpt, typeOpt, includeDeleted)
    } yield SeqHistory(historySequence)
  }

  def retrieveColumnHistory(
      tableId: TableId,
      columnId: ColumnId,
      langtagOpt: Option[Langtag],
      typeOpt: Option[String],
      includeDeleted: Boolean = false
  )(implicit user: TableauxUser): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"))
    logger.info(s"retrieveColumnHistory $tableId $columnId $langtagOpt $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveColumnHistory(table, columnId, langtagOpt, typeOpt, includeDeleted)
    } yield SeqHistory(historySequence)
  }

  def retrieveRowHistory(
      tableId: TableId,
      rowId: RowId,
      langtagOpt: Option[Langtag],
      typeOpt: Option[String],
      includeDeleted: Boolean = false
  )(implicit user: TableauxUser): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveRowHistory $tableId $rowId $langtagOpt $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveRowHistory(table, rowId, langtagOpt, typeOpt, includeDeleted)
    } yield {
      SeqHistory(historySequence)
    }
  }

  def retrieveTableHistory(
      tableId: TableId,
      langtagOpt: Option[Langtag],
      typeOpt: Option[String],
      includeDeleted: Boolean = false
  )(implicit user: TableauxUser): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTableHistory $tableId $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveTableHistory(table, langtagOpt, typeOpt, includeDeleted)
    } yield SeqHistory(historySequence)
  }

  def addRowPermissions(tableId: TableId, rowId: RowId, rowPermissions: Seq[String])(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"addRowPermissions $tableId $rowId $rowPermissions")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- repository.addRowPermissions(table, rowId, rowPermissions)
    } yield EmptyObject()
  }

  def deleteRowPermissions(tableId: TableId, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"removeRowPermissions $tableId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- repository.deleteRowPermissions(table, rowId)
    } yield EmptyObject()
  }

  def replaceRowPermissions(tableId: TableId, rowId: RowId, rowPermissions: Seq[String])(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"replaceRowPermissions $tableId $rowId $rowPermissions")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- repository.replaceRowPermissions(table, rowId, rowPermissions)
    } yield EmptyObject()
  }
}
