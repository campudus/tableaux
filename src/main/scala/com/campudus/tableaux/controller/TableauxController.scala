package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database.domain.DisplayInfos.Langtag
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.{Attachment, TableauxModel}
import com.campudus.tableaux.database.{LanguageNeutral, LocationType}
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.{RequestContext, TableauxConfig, UnprocessableEntityException}
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

object TableauxController {

  def apply(config: TableauxConfig, repository: TableauxModel, roleModel: RoleModel)(
      implicit requestContext: RequestContext): TableauxController = {
    new TableauxController(config, repository, roleModel)
  }
}

class TableauxController(
    override val config: TableauxConfig,
    override protected val repository: TableauxModel,
    implicit protected val roleModel: RoleModel
)(implicit requestContext: RequestContext)
    extends Controller[TableauxModel] {

  def addCellAnnotation(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  ): Future[CellLevelAnnotation] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"addCellAnnotation $tableId $columnId $rowId $langtags $annotationType $value")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ = if (column.languageType == LanguageNeutral && langtags.nonEmpty) {
        throw UnprocessableEntityException(
          s"Cannot add an annotation with langtags to a language neutral cell (table: $tableId, column: $columnId)")
      }
      _ <- roleModel.checkAuthorization(EditCellAnnotation, ScopeTable, ComparisonObjects(table))

      cellAnnotation <- repository.addCellAnnotation(column, rowId, langtags, annotationType, value)
    } yield cellAnnotation
  }

  def deleteCellAnnotation(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: UUID): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"deleteCellAnnotation $tableId $columnId $rowId $uuid")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(EditCellAnnotation, ScopeTable, ComparisonObjects(table))
      _ <- repository.deleteCellAnnotation(column, rowId, uuid)
    } yield EmptyObject()
  }

  def deleteCellAnnotation(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      uuid: UUID,
      langtag: String
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId),
                   greaterThan(columnId, -1, "columnId"),
                   greaterZero(rowId),
                   notNull(langtag, "langtag"))
    logger.info(s"deleteCellAnnotation $tableId $columnId $rowId $uuid $langtag")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ = if (column.languageType == LanguageNeutral) {
        throw UnprocessableEntityException(
          s"There are no annotations with langtags on a language neutral cell (table: $tableId, column: $columnId)")
      }
      _ <- roleModel.checkAuthorization(EditCellAnnotation, ScopeTable, ComparisonObjects(table))

      _ <- repository.deleteCellAnnotation(column, rowId, uuid, langtag)
    } yield EmptyObject()
  }

  def retrieveTableWithCellAnnotations(tableId: TableId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTableWithCellAnnotations $tableId")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(View, ScopeTable, ComparisonObjects(table))

      annotations <- repository.retrieveTableWithCellAnnotations(table)
    } yield annotations
  }

  def retrieveTablesWithCellAnnotationCount(): Future[DomainObject] = {
    logger.info(s"retrieveTablesWithCellAnnotationCount")

    for {
      tables <- repository.retrieveTables()

      tablesWithCellAnnotationCount <- if (tables.isEmpty) {
        Future.successful(Seq.empty[TableWithCellAnnotationCount])
      } else {
        repository.retrieveTablesWithCellAnnotationCount(tables)
      }
    } yield {
      PlainDomainObject(Json.obj("tables" -> tablesWithCellAnnotationCount.map(_.getJson)))
    }
  }

  def retrieveTranslationStatus(): Future[DomainObject] = {
    logger.info(s"retrieveTranslationStatus")

    for {
      tables <- repository.retrieveTables()

      tablesWithMultiLanguageColumnCount <- Future.sequence(
        tables.map(
          table =>
            repository
              .retrieveColumns(table)
              .map(columns => {
                val multiLanguageColumnsCount = columns.count({
                  case MultiLanguageColumn(_) => true
                  case _ => false
                })

                (table, multiLanguageColumnsCount)
              })
        )
      )

      relevantTables = tablesWithMultiLanguageColumnCount.filter({
        case (_, count) if count > 0 => true
        case _ => false
      })

      tablesWithCellAnnotationCount <- repository.retrieveTablesWithCellAnnotationCount(relevantTables.unzip._1)
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

              val percentage = if (count > 0 && multiLanguageCellCount > 0) {
                1.0 - (count.toDouble / multiLanguageCellCount.toDouble)
              } else {
                1.0
              }

              (langtag, percentage)
            })

          (table, multiLanguageColumnsCount, totalSize, needsTranslationStatusByLangtag)
      })

      val translationStatusByTableJson = translationStatusByTable.map({
        case (table, _, _, needsTranslationStatusForLangtags) =>
          table.getJson.mergeIn(
            Json.obj(
              "translationStatus" -> Json.obj(needsTranslationStatusForLangtags: _*)
            ))
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

        (langtag, mergedTranslationStatusForLangtag.sum / mergedTranslationStatusForLangtag.size)
      })

      PlainDomainObject(
        Json.obj(
          "tables" -> translationStatusByTableJson,
          "translationStatus" -> Json.obj(translationStatus: _*)
        )
      )
    }
  }

  def createRow(tableId: TableId, values: Option[Seq[Seq[(ColumnId, _)]]]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))

    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(CreateRow, ScopeTable, ComparisonObjects(table))
      row <- values match {
        case Some(seq) =>
          checkArguments(nonEmpty(seq, "rows"))
          logger.info(s"createRows ${table.id} $values")
          repository.createRows(table, seq)
        case None =>
          logger.info(s"createRow ${table.id}")
          repository.createRow(table)
      }
    } yield row
  }

  def updateRowAnnotations(tableId: TableId, rowId: RowId, finalFlag: Option[Boolean]): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"updateRowAnnotations $tableId $rowId $finalFlag")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(EditRowAnnotation, ScopeTable, ComparisonObjects(table))
      updatedRow <- repository.updateRowAnnotations(table, rowId, finalFlag)
    } yield updatedRow
  }

  def updateRowsAnnotations(tableId: TableId, finalFlag: Option[Boolean]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"updateRowsAnnotations $tableId $finalFlag")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(EditRowAnnotation, ScopeTable, ComparisonObjects(table))
      _ <- repository.updateRowsAnnotations(table, finalFlag)
    } yield EmptyObject()
  }

  def duplicateRow(tableId: TableId, rowId: RowId): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"duplicateRow $tableId $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      duplicated <- repository.duplicateRow(table, rowId)
    } yield duplicated
  }

  def retrieveRow(tableId: TableId, rowId: RowId): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveRow $tableId $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      row <- repository.retrieveRow(table, rowId)
    } yield row
  }

  def retrieveRows(tableId: TableId, pagination: Pagination): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), pagination.check)
    logger.info(s"retrieveRows $tableId for all columns")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, pagination)
    } yield rows
  }

  def retrieveRowsOfFirstColumn(tableId: TableId, pagination: Pagination): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), pagination.check)
    logger.info(s"retrieveRowsOfFirstColumn $tableId for first column")

    for {
      table <- repository.retrieveTable(tableId)
      columns <- repository.retrieveColumns(table)
      rows <- repository.retrieveRows(table, columns.head.id, pagination)
    } yield rows
  }

  def retrieveRowsOfColumn(tableId: TableId, columnId: ColumnId, pagination: Pagination): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), pagination.check)
    logger.info(s"retrieveRows $tableId for column $columnId")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, columnId, pagination)
    } yield rows
  }

  def retrieveForeignRows(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      pagination: Pagination
  ): Future[RowSeq] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId), pagination.check)
    logger.info(s"retrieveForeignRows $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveForeignRows(table, columnId, rowId, pagination)
    } yield rows
  }

  def retrieveCell(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"retrieveCell $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      cell <- repository.retrieveCell(table, columnId, rowId)
    } yield cell
  }

  def retrieveDependentRows(tableId: TableId, rowId: RowId): Future[DependentRowsSeq] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveDependentRows $tableId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(View, ScopeTable, ComparisonObjects(table))
      dependentRows <- repository.retrieveDependentRows(table, rowId)
    } yield dependentRows
  }

  def deleteRow(tableId: TableId, rowId: RowId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"deleteRow $tableId $rowId")
    for {
      table <- repository.retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(DeleteRow, ScopeTable, ComparisonObjects(table))
      _ <- repository.deleteRow(table, rowId)
    } yield EmptyObject()
  }

  def deleteLink(tableId: TableId, columnId: ColumnId, rowId: RowId, toId: RowId): Future[Cell[_]] = {
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
  ): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), greaterZero(toId))
    logger.info(s"updateCellLinkOrder $tableId $columnId $rowId $toId $locationType")
    for {
      table <- repository.retrieveTable(tableId)
      filled <- repository.updateCellLinkOrder(table, columnId, rowId, toId, locationType)
    } yield filled
  }

  def replaceCellValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"replaceCellValue $tableId $columnId $rowId $value")
    for {
      table <- repository.retrieveTable(tableId)
      filled <- repository.replaceCellValue(table, columnId, rowId, value)
    } yield filled
  }

  def updateCellValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"updateCellValue $tableId $columnId $rowId $value")

    for {
      table <- repository.retrieveTable(tableId)
      updated <- repository.updateCellValue(table, columnId, rowId, value)
    } yield updated
  }

  def clearCellValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"clearCellValue $tableId $columnId $rowId")

    for {
      table <- repository.retrieveTable(tableId)
      cleared <- repository.clearCellValue(table, columnId, rowId)
    } yield cleared
  }

  def deleteAttachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: String): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(uuid, "uuid"))
    logger.info(s"deleteAttachment $tableId $columnId $rowId $uuid")

    //TODO introduce a Cell identifier with tableId, columnId and rowId
    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)

      _ <- roleModel.checkAuthorization(EditCellValue, ScopeColumn, ComparisonObjects(table, column))

      _ <- repository.createHistoryModel.createCellsInit(table, rowId, Seq((column, uuid)))
      _ <- repository.attachmentModel.delete(Attachment(tableId, columnId, rowId, UUID.fromString(uuid), None))
      _ <- CacheClient(this).invalidateCellValue(tableId, columnId, rowId)
      _ <- repository.createHistoryModel.createCells(table, rowId, Seq((column, uuid)))
    } yield EmptyObject()
  }

  def retrieveCompleteTable(tableId: TableId): Future[CompleteTable] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveCompleteTable $tableId")

    for {
      table <- repository.retrieveTable(tableId)
      colList <- repository.retrieveColumns(table)
      rowList <- repository.retrieveRows(table, Pagination(None, None))
    } yield CompleteTable(table, colList, rowList)
  }

  def createCompleteTable(tableName: String, columns: Seq[CreateColumn], rows: Seq[Seq[_]]): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $columns rows $rows")

    for {
      table <- repository.createTable(tableName, hidden = false)
      columnIds <- repository.createColumns(table, columns).map(_.map(_.id))
      _ <- repository.createRows(table, rows.map(columnIds.zip(_)))
      completeTable <- retrieveCompleteTable(table.id)
    } yield completeTable
  }

  def retrieveColumnValues(tableId: TableId, columnId: ColumnId, langtagOpt: Option[Langtag]): Future[DomainObject] = {
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
      typeOpt: Option[String]
  ): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"), greaterZero(rowId))
    logger.info(s"retrieveCellHistory $tableId $columnId $rowId $langtagOpt $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveCellHistory(table, columnId, rowId, langtagOpt, typeOpt)
    } yield SeqHistory(historySequence)
  }

  def retrieveRowHistory(
      tableId: TableId,
      rowId: RowId,
      langtagOpt: Option[Langtag],
      typeOpt: Option[String]
  ): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveRowHistory $tableId $rowId $langtagOpt $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveRowHistory(table, rowId, langtagOpt, typeOpt)
    } yield {
      SeqHistory(historySequence)
    }
  }

  def retrieveTableHistory(
      tableId: TableId,
      langtagOpt: Option[Langtag],
      typeOpt: Option[String]
  ): Future[SeqHistory] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTableHistory $tableId $typeOpt")

    for {
      table <- repository.retrieveTable(tableId)
      historySequence <- repository.retrieveTableHistory(table, langtagOpt, typeOpt)
    } yield SeqHistory(historySequence)
  }
}
