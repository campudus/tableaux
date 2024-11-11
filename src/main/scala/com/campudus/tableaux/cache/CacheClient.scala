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

  def setRowPermissions(
      tableId: TableId,
      rowId: RowId,
      rowPermissions: RowPermissions
  ): Future[_] = {
    val rowValue = Json.obj("value" -> rowPermissions.rowPermissions)
    val obj = rowKey(tableId, rowId).copy().mergeIn(rowValue)
    eventBus.sendFuture(CacheVerticle.ADDRESS_SET_ROW_PERMISSIONS, obj, options)
  }

  def retrieveRowPermissions(tableId: TableId, rowId: RowId): Future[Option[RowPermissions]] = {
    val obj = rowKey(tableId, rowId)

    eventBus
      .sendFuture[JsonObject](CacheVerticle.ADDRESS_RETRIEVE_ROW_PERMISSIONS, obj, options)
      .map(value => {
        value match {
          case v if v.body().containsKey("value") => {
            val array = v.body().getValue("value").asInstanceOf[JsonArray]
            Some(RowPermissions(array))
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

  def invalidateRowPermissions(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_ROW_PERMISSIONS, obj)
  }

  def setRowLevelAnnotations(
      tableId: TableId,
      rowId: RowId,
      rowLevelAnnotations: RowLevelAnnotations
  ): Future[_] = {
    val rowValue = Json.obj("value" -> rowLevelAnnotations.getJson)
    val obj = rowKey(tableId, rowId).copy().mergeIn(rowValue)
    eventBus.sendFuture(CacheVerticle.ADDRESS_SET_ROW_LEVEL_ANNOTATIONS, obj, options)
  }

  def retrieveRowLevelAnnotations(tableId: TableId, rowId: RowId): Future[Option[RowLevelAnnotations]] = {
    val obj = rowKey(tableId, rowId)

    eventBus
      .sendFuture[JsonObject](CacheVerticle.ADDRESS_RETRIEVE_ROW_LEVEL_ANNOTATIONS, obj, options)
      .map(value => {
        value match {
          case v if v.body().containsKey("value") => {
            val rawRowLevelAnnotations = v.body().getValue("value").asInstanceOf[JsonObject]
            val finalFlag = rawRowLevelAnnotations.getBoolean("final", false)
            val archivedFlag = rawRowLevelAnnotations.getBoolean("archived", false)
            Some(RowLevelAnnotations(finalFlag, archivedFlag))
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

  def invalidateRowLevelAnnotations(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_ROW_LEVEL_ANNOTATIONS, obj)
  }

  def invalidateTableRowLevelAnnotations(tableId: TableId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId)
    eventBus.sendFuture(CacheVerticle.ADDRESS_INVALIDATE_TABLE_ROW_LEVEL_ANNOTATIONS, obj)
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
