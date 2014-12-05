package com.campudus.tableaux

import com.campudus.tableaux.database.Table
import org.vertx.java.core.json.JsonObject
import org.vertx.scala.router.routing._
import scala.concurrent.{Promise, Future}
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.core.json.Json
import org.vertx.scala.core.VertxExecutionContext
import org.vertx.scala.core.Vertx

class TableauxController(verticle: Starter) {
  implicit val executionContext = VertxExecutionContext.fromVertxAccess(verticle)
  
  def createStringColumn(json: JsonObject): Future[Reply] = {
    val tableId = json.getLong("tableId")
    val columnName = json.getString("columnName")
    
//    for {
//      table <- Table.addColumn(tableId, columnName)
//    } yield 
    
    Future.successful(Ok(Json.obj("tableId"-> 1, "columnId" -> 1)))
  }
  
  def createTable(json: JsonObject, vertx: Vertx): Future[Reply] = {
    val name = json.getString("tableName")
    for {
      table <- Table.create(name, vertx)
    } yield Ok(Json.obj("id" -> table.id))
  }

  def getJson(req: HttpServerRequest): Future[JsonObject] = {
    val p = Promise[JsonObject]
    req.bodyHandler { buf =>
      p.success(Json.fromObjectString(buf.toString()))
    }
    p.future
  }
}