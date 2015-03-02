package com.campudus.tableaux

import com.campudus.tableaux.database._
import scala.concurrent.Future
import org.vertx.scala.core.json.JsonArray
import org.vertx.scala.platform.Verticle
import com.campudus.tableaux.ArgumentChecker._

class TableauxController(verticle: Verticle) {

  val tableaux = new Tableaux(verticle)

  def createColumn(tableId: => Long, namesAndTypes: => Seq[(String, TableauxDbType)]): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), nonEmpty(namesAndTypes, "columns"))
    verticle.logger.info(s"createColumn $tableId $namesAndTypes")
    tableaux.addColumn(tableId, namesAndTypes)
  }

  def createColumn(tableId: Long, name: String, columnType: TableauxDbType, toTable: Long, toColumn: Long, fromColumn: Long): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), notNull(name, "name"), notNull(columnType, "columnType"), greaterZero(toTable), greaterZero(toColumn), greaterZero(fromColumn))
    verticle.logger.info(s"createColumn $tableId $name $columnType $toTable $toColumn $fromColumn")
    tableaux.addLinkColumn(tableId, name, fromColumn, toTable, toColumn)
  }

  def createTable(name: String): Future[DomainObject] = {
    checkArguments(notNull(name, "name"))
    verticle.logger.info(s"createTable $name")
    tableaux.createTable(name)
  }

  def createTable(name: String, columnsNameAndType: Seq[(String, TableauxDbType)], rowsWithColumnsIdAndValue: Seq[Seq[(Long, _)]]): Future[DomainObject] = {
    checkArguments(notNull(name, "name"), nonEmpty(columnsNameAndType, "columns"))
    verticle.logger.info(s"createTable $name $columnsNameAndType $rowsWithColumnsIdAndValue")
    tableaux.createCompleteTable(name, columnsNameAndType, rowsWithColumnsIdAndValue)
  }

  def createRow(tableId: Long, values: Option[Seq[Seq[(Long, _)]]]): Future[DomainObject] = {
    values match {
      case Some(seq) =>
        checkArguments(greaterZero(tableId), nonEmpty(seq, "rows"))
        verticle.logger.info(s"createFullRow $tableId $values")
        tableaux.addFullRows(tableId, seq)
      case None =>
        checkArguments(greaterZero(tableId))
        verticle.logger.info(s"createRow $tableId")
        tableaux.addRow(tableId)
    }
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
    tableaux.deleteTable(tableId)
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

  def fillCell[A](tableId: Long, columnId: Long, rowId: Long, columnType: TableauxDbType, value: A): Future[DomainObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(columnType, "columnType"), notNull(value, "value"))
    verticle.logger.info(s"fillCell $tableId $columnId $rowId $columnType $value")

    columnType match {
      case LinkType =>
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