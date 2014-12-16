package com.campudus.tableaux

import com.campudus.tableaux.database._
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.router.routing._
import scala.concurrent.{ Promise, Future }
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.Vertx
import org.vertx.scala.router.RouterException

class TableauxController(verticle: Starter) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)
  val tableaux = new Tableaux(verticle)

  def createColumn(json: JsonObject): Future[Reply] = {
    val tableId = json.getLong("tableId")
    val columnName = json.getString("columnName")
    val columnType = json.getString("type")
    val (colApply, dbType) = Mapper.ctype(columnType)

    verticle.logger.info(s"createColumn $tableId $columnName $columnType")
    tableaux.addColumn(tableId, columnName, dbType) map { column => Ok(Json.obj("tableId" -> column.table.id, "columnId" -> column.id, "columnType" -> dbType)) }
  }

  def createTable(json: JsonObject): Future[Reply] = {
    val name = json.getString("tableName")

    verticle.logger.info(s"createTable $name")
    tableaux.create(name) map { table => Ok(Json.obj("tableId" -> table.id)) }
  }

  def createRow(json: JsonObject): Future[Reply] = {
    val tableId = json.getLong("tableId")

    verticle.logger.info(s"createRow $tableId")
    tableaux.addRow(tableId) map { rowId => Ok(Json.obj("tableId" -> tableId, "rowId" -> rowId)) }
  }

  def getTable(json: JsonObject): Future[Reply] = {
    val id = json.getLong("tableId")

    verticle.logger.info(s"getTable $id")
    
    for {
    	(table, columnList) <- tableaux.getCompleteTable(id)
      columnJson <- Future.successful{columnList map {case (x, _) => Json.obj("id" -> x.id, "name" -> x.name)}}
      rowJson <- Future.successful{columnList map {case (c, i) => (c, i) } flatMap {case (c, x) => x map { j => Json.obj("id" -> j.rowId, s"c${c.id}" -> j.value)}}}
    } yield Ok(Json.obj("tableId" -> table.id, "tableName" -> table.name, "cols" -> columnJson, "rows" -> rowJson))

    // table.columns map { x => Json.obj("columnId" -> x.columnId.get, "columnName" -> x.name) }
  }

  def getColumn(json: JsonObject): Future[Reply] = {
    val tableId = json.getLong("tableId")
    val columnId = json.getLong("columnId")

    verticle.logger.info(s"getColumn $tableId $columnId")
    tableaux.getColumn(tableId, columnId) map { column => Ok(Json.obj("columnId" -> column.id, "columnName" -> column.name, "type" -> column.dbType)) }
  }

  def deleteTable(json: JsonObject): Future[Reply] = {
    val id = json.getLong("tableId")

    verticle.logger.info(s"deleteTable $id")
    tableaux.delete(id) map { _ => Ok(Json.obj()) }
  }

  def deleteColumn(json: JsonObject): Future[Reply] = {
    val tableId = json.getLong("tableId")
    val columnId = json.getLong("columnId")

    verticle.logger.info(s"deleteColumn $tableId $columnId")
    tableaux.removeColumn(tableId, columnId) map { _ => Ok(Json.obj()) }
  }

  def fillCell(json: JsonObject): Future[Reply] = {
    val tableId = json.getLong("tableId")
    val columnId = json.getLong("columnId")
    val rowId = json.getLong("rowId")
    val columnType = json.getString("type")
    val (colApply, dbType) = Mapper.ctype(columnType)

    val value = dbType match {
      case "text"    => json.getString("value")
      case "numeric" => json.getNumber("value")
    }

    verticle.logger.info(s"fillRow $tableId $columnId $rowId $columnType")
    tableaux.insertValue[value.type, ColumnType[value.type]](tableId, columnId, rowId, value) map { cell =>
      Ok(Json.obj("tableId" -> cell.column.table.id, "columnId" -> cell.column.id, "rowId" -> cell.rowId, "value" -> cell.value))
    }
  }

  def getJson(req: HttpServerRequest): Future[JsonObject] = {
    val p = Promise[JsonObject]
    req.bodyHandler { buf =>
      p.success(Json.fromObjectString(buf.toString()))
    }
    p.future
  }
}