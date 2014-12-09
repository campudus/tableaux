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
    val tableId = json.getString("tableId").toLong
    val columnName = json.getString("columnName")
    verticle.logger.info(s"createColumn $tableId $columnName")
    json.getString("type") match {
      case "text" => for {
        column <- tableaux.addColumn[StringColumn](tableId, columnName, "text")
      } yield Ok(Json.obj("tableId" -> column.table.id, "columnId" -> column.columnId.get, "columnType" -> "text"))
      case "numeric" => for {
        column <- tableaux.addColumn[NumberColumn](tableId, columnName, "numeric")
      } yield Ok(Json.obj("tableId" -> column.table.id, "columnId" -> column.columnId.get, "columnType" -> "numeric"))
      case "Link" => Future.successful(Error(RouterException("not implemented yet.")))
    }
  }

  def createTable(json: JsonObject): Future[Reply] = {
    val name = json.getString("tableName")
    verticle.logger.info(s"createTable $name")
    for {
      table <- tableaux.create(name)
    } yield Ok(Json.obj("tableId" -> table.id))
  }

  def getTable(json: JsonObject): Future[Reply] = {
    val id = json.getString("tableId").toLong
    for {
      table <- tableaux.getTable(id)
    } yield Ok(Json.obj("tableId" -> table.id, "tableName" -> table.name))
  }

  def getJson(req: HttpServerRequest): Future[JsonObject] = {
    val p = Promise[JsonObject]
    req.bodyHandler { buf =>
      p.success(Json.fromObjectString(buf.toString()))
    }
    p.future
  }
}