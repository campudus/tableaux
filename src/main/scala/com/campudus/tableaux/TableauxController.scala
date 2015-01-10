package com.campudus.tableaux

import com.campudus.tableaux.database._
import scala.concurrent.{ Promise, Future }
import org.vertx.scala.core.json.{ Json, JsonObject, JsonArray }
import org.vertx.scala.mods.ScalaBusMod
import org.vertx.scala.core.eventbus.Message
import org.vertx.scala.mods.replies._
import org.vertx.scala.platform.Verticle

class TableauxController(verticle: Verticle) {

  val tableaux = new Tableaux(verticle)

  def createColumn(tableId: Long, name: String, cType: String): Future[DomainObject] = {
    checkIllegalArgument(List(tableId, name, cType))
    verticle.logger.info(s"createColumn $tableId $name $cType")
    tableaux.addColumn(tableId, name, cType)
  }

  def createColumn(tableId: Long, name: String, cType: String, toTable: Long, toColumn: Long, fromColumn: Long): Future[DomainObject] = {
    checkIllegalArgument(List(tableId, name, cType, toTable, toColumn, fromColumn))
    verticle.logger.info(s"createColumn $tableId $name $cType")
    tableaux.addLinkColumn(tableId, name, fromColumn, toTable, toColumn)
  }

  def createTable(name: String): Future[DomainObject] = {
    checkIllegalArgument(List(name))
    verticle.logger.info(s"createTable $name")
    tableaux.create(name)
  }

  def createRow(tableId: Long): Future[DomainObject] = {
    checkIllegalArgument(List(tableId))
    verticle.logger.info(s"createRow $tableId")
    tableaux.addRow(tableId)
  }

  def getTable(tableId: Long): Future[DomainObject] = {
    checkIllegalArgument(List(tableId))
    verticle.logger.info(s"getTable $tableId")
    tableaux.getCompleteTable(tableId)
  }

  def getColumn(tableId: Long, columnId: Long): Future[DomainObject] = {
    checkIllegalArgument(List(tableId, columnId))
    verticle.logger.info(s"getColumn $tableId $columnId")
    tableaux.getColumn(tableId, columnId)
  }

  def deleteTable(tableId: Long): Future[Unit] = {
    checkIllegalArgument(List(tableId))
    verticle.logger.info(s"deleteTable $tableId")
    tableaux.delete(tableId)
  }

  def deleteColumn(tableId: Long, columnId: Long): Future[Unit] = {
    checkIllegalArgument(List(tableId, columnId))
    verticle.logger.info(s"deleteColumn $tableId $columnId")
    tableaux.removeColumn(tableId, columnId)
  }

  def fillCell[A](tableId: Long, columnId: Long, rowId: Long, columnType: String, value: A): Future[DomainObject] = {
    checkIllegalArgument(List(tableId, columnId, rowId, columnType, value))
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

  def resetDB(): Future[Unit] = {
    verticle.logger.info("reset Database")
    tableaux.resetDB()
  }
  
  private def checkIllegalArgument(list: List[_]) = {
    list foreach {
      case null              => throw new IllegalArgumentException
      case x: Long if x <= 0 => throw new IllegalArgumentException
      case _                 =>
    }
  }
}