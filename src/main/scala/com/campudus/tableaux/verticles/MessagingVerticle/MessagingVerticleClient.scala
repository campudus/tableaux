package com.campudus.tableaux.verticles.MessagingVerticle

import com.campudus.tableaux.database.domain.{ColumnType, Table}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess

import io.vertx.core.json.JsonObject
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.EventBus
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

object MessagingVerticleClient {

  def apply(vertx: Vertx): MessagingVerticleClient = {
    new MessagingVerticleClient(vertx)
  }
}

class MessagingVerticleClient(val vertx: Vertx) extends VertxAccess {
  import MessagingVerticle._

  val eventBus: EventBus = vertx.eventBus()

  private def sendMessage(address: String, jsonObj: JsonObject = Json.obj()): Future[Unit] = {
    // catch and ignore all exceptions here in order to not interrupt the main server
    // error handling and logging gets handled in MessagingVerticle
    eventBus.sendFuture[String](address, jsonObj).map(m => {}).recover { case err: Throwable => {} }
  }

  def cellChanged(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId, KEY_ROW_ID -> rowId, KEY_COLUMN_ID -> columnId)
    sendMessage(MessagingVerticle.ADDRESS_CELL_CHANGED, message)
  }

  def servicesChange(): Future[Unit] = {
    sendMessage(MessagingVerticle.ADDRESS_SERVICES_CHANGE)
  }

  def columnCreated(tableId: TableId, columnId: ColumnId): Future[Unit] = {
    val message = Json.obj(KEY_COLUMN_ID -> columnId, KEY_TABLE_ID -> tableId)
    sendMessage(MessagingVerticle.ADDRESS_COLUMN_CREATED, message)
  }

  def columnChanged(tableId: TableId, columnId: ColumnId): Future[Unit] = {
    val message = Json.obj(KEY_COLUMN_ID -> columnId, KEY_TABLE_ID -> tableId)
    sendMessage(MessagingVerticle.ADDRESS_COLUMN_CHANGED, message)
  }

  def columnDeleted(tableId: TableId, columnId: ColumnId, column: ColumnType[_]): Future[Unit] = {
    val message = Json.obj(KEY_COLUMN_ID -> columnId, KEY_TABLE_ID -> tableId, "column" -> column.getJson)
    sendMessage(MessagingVerticle.ADDRESS_COLUMN_DELETED, message)
  }

  def tableCreated(tableId: TableId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId)
    sendMessage(MessagingVerticle.ADDRESS_TABLE_CREATED, message)
  }

  def tableChanged(tableId: TableId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId)
    sendMessage(MessagingVerticle.ADDRESS_TABLE_CHANGED, message)
  }

  def tableDeleted(tableId: TableId, table: Table): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId, "table" -> table.getJson)
    sendMessage(MessagingVerticle.ADDRESS_TABLE_DELETED, message)
  }

  def rowDeleted(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId, KEY_ROW_ID -> rowId)
    sendMessage(MessagingVerticle.ADDRESS_ROW_DELETED, message)
  }

  def rowCreated(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId, KEY_ROW_ID -> rowId)
    sendMessage(MessagingVerticle.ADDRESS_ROW_CREATED, message)
  }

  def cellAnnotationChanged(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId, KEY_ROW_ID -> rowId, KEY_COLUMN_ID -> columnId)
    sendMessage(MessagingVerticle.ADDRESS_CELL_ANNOTATION_CHANGED, message)
  }

  def rowAnnotationChanged(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj(KEY_TABLE_ID -> tableId, KEY_ROW_ID -> rowId)
    sendMessage(MessagingVerticle.ADDRESS_ROW_ANNOTATION_CHANGED, message)
  }
}
