package com.campudus.tableaux

import com.campudus.tableaux.database._
import org.vertx.scala.core.json.JsonObject
import org.vertx.scala.router.routing._
import scala.concurrent.{Promise, Future}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.Vertx

class TableauxController(verticle: Starter) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)
  val tableaux = new Tableaux(verticle)

  def createStringColumn(json: JsonObject): Future[Reply] = {
    val tableId = json.getString("tableId").toLong
    val columnName = json.getString("columnName")
    
//    for {
//      table <- Table.addColumn(tableId, columnName)
//    } yield 
    
    Future.successful(Ok(Json.obj("tableId"-> 1, "columnId" -> 1)))
  }
  
  def createTable(json: JsonObject): Future[Reply] = {
    val name = json.getString("tableName")
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