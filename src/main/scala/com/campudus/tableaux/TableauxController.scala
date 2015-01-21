package com.campudus.tableaux

import com.campudus.tableaux.database._
import scala.concurrent.Future
import org.vertx.scala.core.json.JsonArray
import org.vertx.scala.platform.Verticle
import com.campudus.tableaux.ArgumentChecker._

class TableauxController(verticle: Verticle) {

  val tableaux = new Tableaux(verticle)

  def createColumn(tableId: Long, name: String, cType: String): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), notNull(name), notNull(cType))
    verticle.logger.info(s"createColumn $tableId $name $cType")
    tableaux.addColumn(tableId, name, cType)
  }

  def createColumn(tableId: Long, name: String, cType: String, toTable: Long, toColumn: Long, fromColumn: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), notNull(name), notNull(cType), greaterZero(toTable), greaterZero(toColumn), greaterZero(fromColumn))
    verticle.logger.info(s"createColumn $tableId $name $cType $toTable $toColumn $fromColumn")
    tableaux.addLinkColumn(tableId, name, fromColumn, toTable, toColumn)
  }

  def createTable(name: String): Future[DomainObject] = {
    checkArguments(notNull(name))
    verticle.logger.info(s"createTable $name")
    tableaux.create(name)
  }

  def createRow(tableId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"createRow $tableId")
    tableaux.addRow(tableId)
  }

  def createFullRow(tableId: Long, values: Seq[_]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), notNull(values))
    verticle.logger.info(s"createFullRow $tableId $values")
    tableaux.addFullRow(tableId, values)
  }

  def getTable(tableId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")
    tableaux.getCompleteTable(tableId)
  }

  def getColumn(tableId: Long, columnId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    verticle.logger.info(s"getColumn $tableId $columnId")
    tableaux.getColumn(tableId, columnId)
  }

  def getRow(tableId: Long, rowId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    verticle.logger.info(s"getRow $tableId $rowId")
    tableaux.getRow(tableId, rowId)
  }

  def getCell(tableId: Long, columnId: Long, rowId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId))
    verticle.logger.info(s"getCell $tableId $columnId $rowId")
    tableaux.getCell(tableId, columnId, rowId)
  }

  def deleteTable(tableId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    verticle.logger.info(s"deleteTable $tableId")
    tableaux.delete(tableId)
  }

  def deleteColumn(tableId: Long, columnId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    verticle.logger.info(s"deleteColumn $tableId $columnId")
    tableaux.removeColumn(tableId, columnId)
  }

  def deleteRow(tableId: Long, rowId: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(rowId))
    verticle.logger.info(s"deleteRow $tableId $rowId")
    tableaux.deleteRow(tableId, rowId)
  }

  def fillCell[A](tableId: Long, columnId: Long, rowId: Long, columnType: String, value: A): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(columnType), notNull(value))
    verticle.logger.info(s"fillCell $tableId $columnId $rowId $columnType $value")

    Mapper.getDatabaseType(columnType) match {
      case "link" =>
        import scala.collection.JavaConverters._
        val valueList = value.asInstanceOf[JsonArray].asScala.toList.asInstanceOf[List[Number]]
        val valueFromList = (valueList(0).longValue(), valueList(1).longValue())
        tableaux.insertLinkValue(tableId, columnId, rowId, valueFromList)
      case _ => tableaux.insertValue(tableId, columnId, rowId, value)
    }
  }

  def resetDB(): Future[DomainObject] = {
    verticle.logger.info("Reset database")
    tableaux.resetDB()
  }

}