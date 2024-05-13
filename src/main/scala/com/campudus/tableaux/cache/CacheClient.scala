package com.campudus.tableaux.cache

import com.campudus.tableaux.database.domain.{DomainObject, RowLevelAnnotations, RowPermissions}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess

import io.vertx.core.eventbus.ReplyException
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus._
import org.vertx.scala.core.json._

import scala.concurrent.Future

object CacheClient {

  def apply(vertxAccess: VertxAccess): CacheClient = {
    new CacheClient(vertxAccess)
  }
}

class CacheClient(vertxAccess: VertxAccess) extends VertxAccess {

  val options = DeliveryOptions().setSendTimeout(CacheVerticle.TIMEOUT_AFTER_MILLISECONDS)

  override val vertx: Vertx = vertxAccess.vertx

  private val eventBus = vertx.eventBus()

  private def cellKey(tableId: TableId, columnId: ColumnId, rowId: RowId) =
    Json.obj("tableId" -> tableId, "columnId" -> columnId, "rowId" -> rowId)

  private def rowKey(tableId: TableId, rowId: RowId) = Json.obj("tableId" -> tableId, "rowId" -> rowId)

  def setCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId, value: Any): Future[_] = {
    val cellValue = Json.obj("value" -> DomainObject.compatibilityGet(value))
    val obj = cellKey(tableId, columnId, rowId).copy().mergeIn(cellValue)

    eventBus.sendFuture(CacheVerticle.ADDRESS_SET_CELL, obj, options)
  }

  def retrieveCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Option[Any]] = {
    val obj = cellKey(tableId, columnId, rowId)

    eventBus
      .sendFuture[JsonObject](CacheVerticle.ADDRESS_RETRIEVE_CELL, obj, options)
      .map({
        case v if v.body().containsKey("value") =>
          Some(v.body().getValue("value"))
        case _ =>
          None
      })
      .recoverWith({
        case ex: ReplyException if ex.failureCode() == CacheVerticle.NOT_FOUND_FAILURE =>
          Future.successful(None)
        case ex =>
          Future.failed(ex)
      })
  }

  def setRowValues(
      tableId: TableId,
      rowId: RowId,
      rowLevelAnnotations: RowLevelAnnotations,
      rowPermissions: RowPermissions
  ): Future[_] = {
    val rowValue = Json.obj("value" -> Json.obj("rowPermissions" -> rowPermissions.getJson))
    val obj = rowKey(tableId, rowId).copy().mergeIn(rowValue)
    eventBus.sendFuture(CacheVerticle.ADDRESS_SET_ROW, obj, options)
  }

  def retrieveRowValues(tableId: TableId, rowId: RowId): Future[Option[JsonArray]] = {
    val obj = rowKey(tableId, rowId)

    eventBus
      .sendFuture[JsonObject](CacheVerticle.ADDRESS_RETRIEVE_ROW, obj, options)
      .map(value => {
        value match {
          case v if v.body().containsKey("value") => {
            val value = v.body().getValue("value").asInstanceOf[JsonObject]
            val rowPermissions = value.getJsonObject("rowPermissions").getJsonArray("permissions")
            Some(rowPermissions)
          }
          case _ => {
            None
          }
        }
      })
      .recoverWith({
        case ex: ReplyException if ex.failureCode() == CacheVerticle.NOT_FOUND_FAILURE =>
          Future.successful(None)
        case ex =>
          Future.failed(ex)
      })
  }

  def invalidateCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[_] = {
    val obj = cellKey(tableId, columnId, rowId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_CELL, obj, options)
  }

  def invalidateColumn(tableId: TableId, columnId: ColumnId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_COLUMN, obj)
  }

  def invalidateRow(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_ROW, obj)
  }

  def invalidateTable(tableId: TableId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_TABLE, obj)
  }

  def invalidateAll(): Future[_] = {
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_ALL, Json.emptyObj())
  }
}
