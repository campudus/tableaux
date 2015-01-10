package com.campudus.tableaux

import com.campudus.tableaux.database._
import scala.concurrent.Future
import org.vertx.scala.core.json.JsonArray
import org.vertx.scala.platform.Verticle

sealed trait ArgumentCheck

case object OkArg extends ArgumentCheck

case class FailArg(message: String) extends ArgumentCheck

class TableauxController(verticle: Verticle) {

  val tableaux = new Tableaux(verticle)

  def createColumn(tableId: Long, name: String, cType: String): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId), notNull(name), notNull(cType))
    verticle.logger.info(s"createColumn $tableId $name $cType")
    tableaux.addColumn(tableId, name, cType)
  }

  def createColumn(tableId: Long, name: String, cType: String, toTable: Long, toColumn: Long, fromColumn: Long): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId), notNull(name), notNull(cType), greaterZero(toTable), greaterZero(toColumn), greaterZero(fromColumn))
    verticle.logger.info(s"createColumn $tableId $name $cType")
    tableaux.addLinkColumn(tableId, name, fromColumn, toTable, toColumn)
  }

  def createTable(name: String): Future[DomainObject] = {
    checkIllegalArguments(notNull(name))
    verticle.logger.info(s"createTable $name")
    tableaux.create(name)
  }

  def createRow(tableId: Long): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId))
    verticle.logger.info(s"createRow $tableId")
    tableaux.addRow(tableId)
  }

  def getTable(tableId: Long): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId))
    verticle.logger.info(s"getTable $tableId")
    tableaux.getCompleteTable(tableId)
  }

  def getColumn(tableId: Long, columnId: Long): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId), greaterZero(columnId))
    verticle.logger.info(s"getColumn $tableId $columnId")
    tableaux.getColumn(tableId, columnId)
  }

  def deleteTable(tableId: Long): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId))
    verticle.logger.info(s"deleteTable $tableId")
    tableaux.delete(tableId)
  }

  def deleteColumn(tableId: Long, columnId: Long): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId), greaterZero(columnId))
    verticle.logger.info(s"deleteColumn $tableId $columnId")
    tableaux.removeColumn(tableId, columnId)
  }

  def fillCell[A](tableId: Long, columnId: Long, rowId: Long, columnType: String, value: A): Future[DomainObject] = {
    checkIllegalArguments(greaterZero(tableId), greaterZero(columnId), greaterZero(rowId), notNull(columnType), notNull(value))
    import scala.collection.JavaConverters._

    val dbType = Mapper.getDatabaseType(columnType)

    dbType match {
      case "link" =>
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

  private def notNull(x: Any): ArgumentCheck = if (x != null) OkArg else FailArg("Argument is null")

  private def greaterZero(x: Long): ArgumentCheck = if (x > 0) OkArg else FailArg("Argument is not greater Zero")

  private def checkIllegalArguments(args: ArgumentCheck*): Unit = args foreach {
    case FailArg(ex) => throw new IllegalArgumentException(ex)
    case OkArg       =>
  }
}