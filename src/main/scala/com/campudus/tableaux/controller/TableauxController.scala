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

  def getAllTables(): Future[DomainObject] = {
    logger.info("getAllTables")
    repository.getAllTables()
  }

  def createColumn(tableId: => IdType, columns: => Seq[CreateColumn]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), nonEmpty(columns, "columns"))
    logger.info(s"createColumn $tableId $columns")
    repository.addColumns(tableId, columns)
  }

  def createTable(tableName: String): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"))
    logger.info(s"createTable $tableName")
    repository.createTable(tableName)
  }

  def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    repository.createCompleteTable(tableName, columns, rowsValues)
  }

  def createRow(tableId: IdType, values: Option[Seq[Seq[(IdType, _)]]]): Future[DomainObject] = {
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

  def getTable(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")
    repository.getTable(tableId)
  }

  def getCompleteTable(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")
    repository.getCompleteTable(tableId)
  }

  def getColumn(tableId: IdType, columnId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"getColumn $tableId $columnId")
    repository.getColumn(tableId, columnId)
  }

  def getColumns(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"getColumns $tableId")
    repository.getColumns(tableId)
  }

  def getRow(tableId: IdType, rowId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"getRow $tableId $rowId")
    repository.getRow(tableId, rowId)
  }

  def getRows(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"getRows $tableId")
    repository.getRows(tableId)
  }

  def getCell(tableId: IdType, columnId: IdType, rowId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"getCell $tableId $columnId $rowId")
    repository.getCell(tableId, columnId, rowId)
  }

  def deleteTable(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"deleteTable $tableId")
    repository.deleteTable(tableId)
  }

  def deleteColumn(tableId: IdType, columnId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"deleteColumn $tableId $columnId")
    repository.removeColumn(tableId, columnId)
  }

  def deleteRow(tableId: IdType, rowId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"deleteRow $tableId $rowId")
    repository.deleteRow(tableId, rowId)
  }

  def fillCell[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"fillCell $tableId $columnId $rowId $value")
    repository.insertValue(tableId, columnId, rowId, value)
  }

  def updateCell[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[DomainObject] = {
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

  def changeTableName(tableId: IdType, tableName: String): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), notNull(tableName, "TableName"))
    logger.info(s"changeTableName $tableId $tableName")
    repository.changeTableName(tableId, tableName)
  }

  def changeColumn(tableId: IdType,
                   columnId: IdType,
                   columnName: Option[String],
                   ordering: Option[Ordering],
                   kind: Option[TableauxDbType]): Future[DomainObject] = {

    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"changeColumn $tableId $columnId $columnName $ordering $kind")
    repository.changeColumn(tableId, columnId, columnName, ordering, kind)
  }
}