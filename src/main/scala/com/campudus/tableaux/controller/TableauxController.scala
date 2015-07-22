package com.campudus.tableaux.controller

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.model.{Attachment, TableauxModel}
import TableauxModel._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{EmptyObject, DomainObject, CreateColumn}

import scala.concurrent.Future

object TableauxController {
  def apply(config: TableauxConfig, repository: TableauxModel): TableauxController = {
    new TableauxController(config, repository)
  }
}

class TableauxController(override val config: TableauxConfig, override protected val repository: TableauxModel) extends Controller[TableauxModel] {

  def createRow(tableId: TableId, values: Option[Seq[Seq[(ColumnId, _)]]]): Future[DomainObject] = {
    values match {
      case Some(seq) =>
        checkArguments(greaterZero(tableId), nonEmpty(seq, "rows"))
        logger.info(s"createFullRow $tableId $values")
        repository.addFullRows(tableId, seq)
      case None =>
        checkArguments(greaterZero(tableId))
        logger.info(s"createRow $tableId")
        repository.addRow(tableId)
    }
  }

  def getRow(tableId: TableId, rowId: TableId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"getRow $tableId $rowId")
    repository.getRow(tableId, rowId)
  }

  def getRows(tableId: TableId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"getRows $tableId")
    repository.getRows(tableId)
  }

  def getCell(tableId: TableId, columnId: ColumnId, rowId: ColumnId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"getCell $tableId $columnId $rowId")
    repository.getCell(tableId, columnId, rowId)
  }

  def deleteRow(tableId: TableId, rowId: RowId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"deleteRow $tableId $rowId")
    repository.deleteRow(tableId, rowId)
  }

  def fillCell[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"fillCell $tableId $columnId $rowId $value")
    repository.insertValue(tableId, columnId, rowId, value)
  }

  def updateCell[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"updateCell $tableId $columnId $rowId $value")
    repository.updateValue(tableId, columnId, rowId, value)
  }

  def deleteAttachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: String): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(uuid, "uuid"))
    logger.info(s"deleteAttachment $tableId $columnId $rowId $uuid")
    //TODO introduce a Cell identifier with tableId, columnId and rowId
    repository.attachmentModel.delete(Attachment(tableId, columnId, rowId, UUID.fromString(uuid), None))
  }

  private implicit def convertUnitToEmptyObject(unit: Future[Unit]): Future[EmptyObject] = {
    unit map (s => EmptyObject())
  }
}