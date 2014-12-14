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

  def createColumn(json: JsonObject): Future[Reply] = for {
    tableId <- Future.successful { json.getString("tableId").toLong }
    columnName <- Future.successful { json.getString("columnName") }
    _ <- Future.successful { verticle.logger.info(s"createColumn $tableId $columnName") }
    column <- json.getString("type") match {
      case "text" => tableaux.addColumn[StringColumn](tableId, columnName, "text")
      case "numeric" => tableaux.addColumn[NumberColumn](tableId, columnName, "numeric")
//      case "Link" => Future.successful(Error(RouterException("not implemented yet.")))
    }
  } yield Ok(Json.obj("tableId" -> column.table.id, "columnId" -> column.columnId.get, "columnType" -> column.dbType))

  def createTable(json: JsonObject): Future[Reply] = for {
    name <- Future.successful { json.getString("tableName") }
    _ <- Future.successful { verticle.logger.info(s"createTable $name") }
    table <- tableaux.create(name)
  } yield Ok(Json.obj("tableId" -> table.id))

  def getTable(json: JsonObject): Future[Reply] = for {
    id <- Future.successful { json.getString("tableId").toLong }
    _ <- Future.successful { verticle.logger.info(s"getTable $id") }
    table <- tableaux.getTable(id)
    column <- Future.successful {
      table.columns map { x => Json.obj("columnId" -> x.columnId.get, "columnName" -> x.name) }
    }
  } yield Ok(Json.obj("tableId" -> table.id, "tableName" -> table.name, "cols" -> column))

  def getColumn(json: JsonObject): Future[Reply] = for {
    tableId <- Future.successful{ json.getString("tableId").toLong }
    columnId <- Future.successful { json.getString("columnId").toLong }
    _ <- Future.successful { verticle.logger.info(s"getColumn $tableId $columnId") }
    column <- tableaux.getColumn(tableId, columnId)
    reply <- Future.successful {
      column match {
        case c: ValueColumnType => Ok(Json.obj("columnId" -> c.columnId.get, "columnName" -> c.name, "type" -> c.dbType))
        case c: ColumnType => Ok(Json.obj("columnId" -> c.columnId.get, "columnName" -> c.name))
      }
    }
  } yield reply
  
  def deleteTable(json: JsonObject): Future[Reply] = for {
    id <- Future.successful { json.getString("tableId").toLong }
    _ <- Future.successful { verticle.logger.info(s"deleteTable $id") }
    table <- tableaux.delete(id)
  } yield Ok(Json.obj())
  
  def deleteColumn(json: JsonObject): Future[Reply] = for {
    tableId <- Future.successful { json.getString("tableId").toLong }
    columnId <- Future.successful { json.getString("columnId").toLong }
    _ <- Future.successful { verticle.logger.info(s"deleteColumn $tableId $columnId") }
    table <- tableaux.removeColumn(tableId, columnId)
  } yield Ok(Json.obj())

  def getJson(req: HttpServerRequest): Future[JsonObject] = {
    val p = Promise[JsonObject]
    req.bodyHandler { buf =>
      p.success(Json.fromObjectString(buf.toString()))
    }
    p.future
  }
}