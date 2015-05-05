package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database.Tableaux._
import com.campudus.tableaux.database._
import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.platform.Verticle

import scala.concurrent.Future

class TableauxController(val verticle: Verticle, val database: DatabaseConnection) extends DatabaseAccess with StandardVerticle {

  lazy val tableaux = new Tableaux(verticle, database)

  def getAllTables(): Future[DomainObject] = {
    logger.info("getAllTables")
    tableaux.getAllTables()
  }

  def createColumn(tableId: => IdType, columns: => Seq[CreateColumn]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), nonEmpty(columns, "columns"))
    logger.info(s"createColumn $tableId $columns")
    tableaux.addColumns(tableId, columns)
  }

  def createTable(tableName: String): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"))
    logger.info(s"createTable $tableName")
    tableaux.createTable(tableName)
  }

  def createTable(tableName: String, columns: => Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[DomainObject] = {
    checkArguments(notNull(tableName, "TableName"), nonEmpty(columns, "columns"))
    logger.info(s"createTable $tableName columns $rowsValues")
    tableaux.createCompleteTable(tableName, columns, rowsValues)
  }

  def createRow(tableId: IdType, values: Option[Seq[Seq[(IdType, _)]]]): Future[DomainObject] = {
    values match {
      case Some(seq) =>
        checkArguments(greaterZero(tableId), nonEmpty(seq, "rows"))
        logger.info(s"createFullRow $tableId $values")
        tableaux.addFullRows(tableId, seq)
      case None =>
        checkArguments(greaterZero(tableId))
        logger.info(s"createRow $tableId")
        tableaux.addRow(tableId)
    }
  }

  def getTable(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")
    tableaux.getCompleteTable(tableId)
  }

  def getColumn(tableId: IdType, columnId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"getColumn $tableId $columnId")
    tableaux.getColumn(tableId, columnId)
  }

  def getRow(tableId: IdType, rowId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"getRow $tableId $rowId")
    tableaux.getRow(tableId, rowId)
  }

  def getCell(tableId: IdType, columnId: IdType, rowId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    logger.info(s"getCell $tableId $columnId $rowId")
    tableaux.getCell(tableId, columnId, rowId)
  }

  def deleteTable(tableId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"deleteTable $tableId")
    tableaux.deleteTable(tableId)
  }

  def deleteColumn(tableId: IdType, columnId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"deleteColumn $tableId $columnId")
    tableaux.removeColumn(tableId, columnId)
  }

  def deleteRow(tableId: IdType, rowId: IdType): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    logger.info(s"deleteRow $tableId $rowId")
    tableaux.deleteRow(tableId, rowId)
  }

  def fillCell[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(value, "value"))
    logger.info(s"fillCell $tableId $columnId $rowId $value")
    tableaux.insertValue(tableId, columnId, rowId, value)
  }

  def resetDB(): Future[DomainObject] = {
    logger.info("Reset database")
    tableaux.resetDB()
  }

  def changeTableName(tableId: IdType, tableName: String): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), notNull(tableName, "TableName"))
    logger.info(s"changeTableName $tableId $tableName")
    tableaux.changeTableName(tableId, tableName)
  }

  def changeColumn(tableId: IdType, columnId: IdType, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"changeColumn $tableId $columnId $columnName $ordering $kind")
    tableaux.changeColumn(tableId, columnId, columnName, ordering, kind)
  }
}