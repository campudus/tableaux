package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.{TableauxConfig, UnprocessableEntityException}
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database.{LanguageNeutral, LocationType}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.{Attachment, TableauxModel}

import scala.concurrent.Future

object TableauxController {

  def apply(config: TableauxConfig, repository: TableauxModel): TableauxController = {
    new TableauxController(config, repository)
  }
}

class TableauxController(override val config: TableauxConfig, override protected val repository: TableauxModel)
    extends Controller[TableauxModel] {

  def addCellAnnotation(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  ): Future[CellLevelAnnotation] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"addCellAnnotation $tableId $columnId $rowId $langtags $annotationType $value")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ = if (column.languageType == LanguageNeutral && langtags.nonEmpty) {
        throw UnprocessableEntityException("Cannot add an annotation with langtags on a language neutral cell")
      }

      cellAnnotation <- repository.addCellAnnotation(column, rowId, langtags, annotationType, value)
    } yield cellAnnotation
  }

  def deleteCellAnnotation(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: UUID): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"deleteCellAnnotation $tableId $columnId $rowId $uuid")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
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
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"deleteCellAnnotation $tableId $columnId $rowId $uuid $langtag")

    for {
      table <- repository.retrieveTable(tableId)
      column <- repository.retrieveColumn(table, columnId)
      _ <- repository.deleteCellAnnotation(column, rowId, uuid, langtag)
    } yield EmptyObject()
  }

  def createRow(tableId: TableId, values: Option[Seq[Seq[(ColumnId, _)]]]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))

    for {
      table <- repository.retrieveTable(tableId)
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
      updatedRow <- repository.updateRowAnnotations(table, rowId, finalFlag)
    } yield updatedRow
  }

  def updateRowsAnnotations(tableId: TableId, finalFlag: Option[Boolean]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"updateRowsAnnotations $tableId $finalFlag")
    for {
      table <- repository.retrieveTable(tableId)
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
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveRows $tableId for all columns")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, pagination)
    } yield rows
  }

  def retrieveRowsOfFirstColumn(tableId: TableId, pagination: Pagination): Future[RowSeq] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveRowsOfFirstColumn $tableId for first column")

    for {
      table <- repository.retrieveTable(tableId)
      columns <- repository.retrieveColumns(table)
      rows <- repository.retrieveRows(table, columns.head.id, pagination)
    } yield rows
  }

  def retrieveRowsOfColumn(tableId: TableId, columnId: ColumnId, pagination: Pagination): Future[RowSeq] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveRows $tableId for column $columnId")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, columnId, pagination)
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
      dependentRows <- repository.retrieveDependentRows(table, rowId)
    } yield dependentRows
  }

  def deleteRow(tableId: TableId, rowId: RowId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"deleteRow $tableId $rowId")
    for {
      table <- repository.retrieveTable(tableId)
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
      _ <- repository.attachmentModel.delete(Attachment(tableId, columnId, rowId, UUID.fromString(uuid), None))
      _ <- CacheClient(vertx).invalidateCellValue(tableId, columnId, rowId)
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
}
