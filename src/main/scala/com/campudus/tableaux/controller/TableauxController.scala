package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.{Attachment, TableauxModel}

import scala.concurrent.Future

object TableauxController {
  def apply(config: TableauxConfig, repository: TableauxModel): TableauxController = {
    new TableauxController(config, repository)
  }
}

class TableauxController(override val config: TableauxConfig, override protected val repository: TableauxModel) extends Controller[TableauxModel] {

  def createRow(tableId: TableId, values: Option[Seq[Seq[(ColumnId, _)]]]): Future[Either[RowSeq, Row]] = {
    values match {
      case Some(seq) =>
        checkArguments(greaterZero(tableId), nonEmpty(seq, "rows"))
        logger.info(s"createRows $tableId $values")
        repository.createRows(tableId, seq).map(Left.apply)
      case None =>
        checkArguments(greaterZero(tableId))
        logger.info(s"createRow $tableId")
        repository.createRow(tableId).map(Right.apply)
    }
  }

  def duplicateRow(tableId: TableId, rowId: RowId): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"duplicateRow $tableId $rowId")
    repository.duplicateRow(tableId, rowId)
  }

  def retrieveRow(tableId: TableId, rowId: RowId): Future[Row] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"retrieveRow $tableId $rowId")
    repository.retrieveRow(tableId, rowId)
  }

  def retrieveRows(tableId: TableId, pagination: Pagination): Future[RowSeq] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveRows $tableId for all columns")

    for {
      table <- repository.retrieveTable(tableId)
      rows <- repository.retrieveRows(table, pagination)
    } yield rows
  }

  def retrieveRows(tableId: TableId, columnId: ColumnId, pagination: Pagination): Future[RowSeq] = {
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
    repository.retrieveCell(tableId, columnId, rowId)
  }

  def deleteRow(tableId: TableId, rowId: RowId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"deleteRow $tableId $rowId")
    repository.deleteRow(tableId, rowId)
  }

  def fillCell[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"fillCell $tableId $columnId $rowId $value")
    repository.insertValue(tableId, columnId, rowId, value)
  }

  def updateCell[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"updateCell $tableId $columnId $rowId $value")
    repository.updateValue(tableId, columnId, rowId, value)
  }

  def deleteAttachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: String): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(uuid, "uuid"))
    logger.info(s"deleteAttachment $tableId $columnId $rowId $uuid")

    //TODO introduce a Cell identifier with tableId, columnId and rowId
    for {
      _ <- repository.attachmentModel.delete(Attachment(tableId, columnId, rowId, UUID.fromString(uuid), None))
    } yield EmptyObject()
  }

  def retrieveCompleteTable(tableId: TableId): Future[CompleteTable] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveCompleteTable $tableId")

    for {
      table <- repository.retrieveTable(tableId)
      colList <- repository.retrieveColumns(table.id)
      rowList <- repository.retrieveRows(table, Pagination(None, None))
    } yield CompleteTable(table, colList, rowList)
  }

  def createCompleteTable(tableName: String, columns: Seq[CreateColumn], rows: Seq[Seq[_]]): Future[CompleteTable] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $columns rows $rows")

    for {
      table <- repository.createTable(tableName, hidden = false)
      columnIds <- repository.createColumns(table.id, columns).map(_.map(_.id))
      _ <- repository.createRows(table.id, rows.map(columnIds.zip(_)))
      completeTable <- retrieveCompleteTable(table.id)
    } yield completeTable
  }
}