package com.campudus.tableaux.verticles

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.domain.RowLevelAnnotations
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess

import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus._
import io.vertx.scala.core.eventbus.DeliveryOptions
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.reflect.io.Path

import java.util.UUID

object EventClient {
  val ADDRESS_CELL_CHANGED = "cell.changed"
  val ADDRESS_SERVICES_CHANGED = "services.changed"
  val ADDRESS_COLUMN_CREATED = "column.created"
  val ADDRESS_COLUMN_CHANGED = "column.changed"
  val ADDRESS_COLUMN_DELETED = "column.deleted"
  val ADDRESS_TABLE_CREATED = "table.created"
  val ADDRESS_TABLE_CHANGED = "table.changed"
  val ADDRESS_TABLE_DELETED = "table.deleted"
  val ADDRESS_ROW_CREATED = "row.created"
  val ADDRESS_ROW_DELETED = "row.deleted"
  val ADDRESS_ROW_ANNOTATION_CHANGED = "row.annotation.changed"
  val ADDRESS_CELL_ANNOTATION_CHANGED = "cell.annotation.changed"
  val ADDRESS_FILE_CHANGED = "file.changed"
  val ADDRESS_THUMBNAIL_RETRIEVE = "thumbnail.retrieve"

  // json
  val ADDRESS_JSON_SCHEMA_VALIDATE = "json.schema.validate"
  val ADDRESS_JSON_SCHEMA_REGISTER = "json.schema.register"

  // cell based addresses
  val ADDRESS_SET_CELL = "cache.set.cell"
  val ADDRESS_RETRIEVE_CELL = "cache.retrieve.cell"
  val ADDRESS_INVALIDATE_CELL = "cache.invalidate.cell"
  val ADDRESS_INVALIDATE_COLUMN = "cache.invalidate.column"
  val ADDRESS_INVALIDATE_ROW = "cache.invalidate.row"
  val ADDRESS_INVALIDATE_TABLE = "cache.invalidate.table"
  val ADDRESS_INVALIDATE_ALL = "cache.invalidate.all"

  // row permissions based addresses
  val ADDRESS_SET_ROW_PERMISSIONS = "cache.set.row_permissions"
  val ADDRESS_RETRIEVE_ROW_PERMISSIONS = "cache.retrieve.row_permissions"
  val ADDRESS_INVALIDATE_ROW_PERMISSIONS = "cache.invalidate.row_permissions"

  // row level annotations based addresses
  val ADDRESS_SET_ROW_LEVEL_ANNOTATIONS = "cache.set.row_level_annotations"
  val ADDRESS_RETRIEVE_ROW_LEVEL_ANNOTATIONS = "cache.retrieve.row_level_annotations"
  val ADDRESS_INVALIDATE_ROW_LEVEL_ANNOTATIONS = "cache.invalidate.row_level_annotations"
  val ADDRESS_INVALIDATE_TABLE_ROW_LEVEL_ANNOTATIONS = "cache.invalidate.table.row_level_annotations"

  def apply(vertx: Vertx): EventClient = {
    new EventClient(vertx)
  }
}

class EventClient(val vertx: Vertx) extends VertxAccess {
  import EventClient._

  val options = DeliveryOptions().setSendTimeout(CacheVerticle.TIMEOUT_AFTER_MILLISECONDS)

  val eventBus: EventBus = vertx.eventBus()

  private def sendMessage(address: String, jsonObj: JsonObject = Json.obj()): Future[Unit] = {
    /*
     * Catch and ignore all exceptions here in order to not interrupt the main server.
     * Error handling and logging gets handled in Verticle.
     */
    eventBus.sendFuture[String](address, jsonObj).map(_ => {}).recover { case _: Throwable => }
  }

  def cellChanged(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId, "columnId" -> columnId)

    sendMessage(ADDRESS_CELL_CHANGED, message)
  }

  def servicesChanged(): Future[Unit] = {
    sendMessage(ADDRESS_SERVICES_CHANGED)
  }

  def columnCreated(tableId: TableId, columnId: ColumnId): Future[Unit] = {
    val message = Json.obj("columnId" -> columnId, "tableId" -> tableId)

    sendMessage(ADDRESS_COLUMN_CREATED, message)
  }

  def columnChanged(tableId: TableId, columnId: ColumnId): Future[Unit] = {
    val message = Json.obj("columnId" -> columnId, "tableId" -> tableId)

    sendMessage(ADDRESS_COLUMN_CHANGED, message)
  }

  def columnDeleted(tableId: TableId, columnId: ColumnId, column: ColumnType[_]): Future[Unit] = {
    val message = Json.obj("columnId" -> columnId, "tableId" -> tableId, "column" -> column.getJson)

    sendMessage(ADDRESS_COLUMN_DELETED, message)
  }

  def tableCreated(tableId: TableId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId)

    sendMessage(ADDRESS_TABLE_CREATED, message)
  }

  def tableChanged(tableId: TableId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId)

    sendMessage(ADDRESS_TABLE_CHANGED, message)
  }

  def tableDeleted(tableId: TableId, table: Table): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "table" -> table.getJson)

    sendMessage(ADDRESS_TABLE_DELETED, message)
  }

  def rowDeleted(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    sendMessage(ADDRESS_ROW_DELETED, message)
  }

  def rowCreated(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    sendMessage(ADDRESS_ROW_CREATED, message)
  }

  def cellAnnotationChanged(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId, "columnId" -> columnId)

    sendMessage(ADDRESS_CELL_ANNOTATION_CHANGED, message)
  }

  def rowAnnotationChanged(tableId: TableId, rowId: RowId): Future[Unit] = {
    val message = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    sendMessage(ADDRESS_ROW_ANNOTATION_CHANGED, message)
  }

  def fileChanged(oldFile: ExtendedFile): Future[Unit] = {
    val message = oldFile.getJson

    sendMessage(ADDRESS_FILE_CHANGED, message)
  }

  def retrieveThumbnailPath(fileUuid: UUID, langtag: String, width: Int, timeout: Int): Future[Path] = {
    val message = Json.obj("uuid" -> fileUuid.toString, "langtag" -> langtag, "width" -> width)
    val thumbnailOptions = DeliveryOptions().setSendTimeout(timeout)

    eventBus
      .sendFuture[String](ADDRESS_THUMBNAIL_RETRIEVE, message, thumbnailOptions).map(message => Path(message.body()))
  }

  def validateJson(key: String, json: JsonObject): Future[Unit] = {
    val message = Json.obj("key" -> key, "jsonToValidate" -> json, "jsonType" -> "object")

    eventBus
      .sendFuture[String](ADDRESS_JSON_SCHEMA_VALIDATE, message)
      .map(_ => {})
  }

  def validateJson(key: String, json: JsonArray): Future[Unit] = {
    val message = Json.obj("key" -> key, "jsonToValidate" -> json, "jsonType" -> "array")

    eventBus.sendFuture[String](ADDRESS_JSON_SCHEMA_VALIDATE, message).map(_ => {})
  }

  def registerSchema(schemaWithKey: JsonObject): Future[Unit] = {
    eventBus.sendFuture[String](ADDRESS_JSON_SCHEMA_REGISTER, schemaWithKey).map(_ => {})
  }

  def registerMultipleSchemas(schemaWithKeyList: List[JsonObject]): Future[List[Unit]] = {
    Future.sequence(schemaWithKeyList.map(registerSchema))
  }

  def setCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId, value: Any): Future[_] = {
    val cellValue = Json.obj("value" -> DomainObject.compatibilityGet(value))
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId, "rowId" -> rowId).copy().mergeIn(cellValue)

    eventBus.sendFuture(ADDRESS_SET_CELL, obj, options)
  }

  def retrieveCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Option[Any]] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId, "rowId" -> rowId)

    eventBus
      .sendFuture[JsonObject](ADDRESS_RETRIEVE_CELL, obj, options)
      .map({
        case v if v.body().containsKey("value") => Some(v.body().getValue("value"))
        case _ => None
      })
      .recoverWith({
        case ex: ReplyException if ex.failureCode() == 404 => Future.successful(None)
        case ex => Future.failed(ex)
      })
  }

  def setRowPermissions(
      tableId: TableId,
      rowId: RowId,
      rowPermissions: RowPermissions
  ): Future[_] = {
    val rowValue = Json.obj("value" -> rowPermissions.rowPermissions)
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId).copy().mergeIn(rowValue)

    eventBus.sendFuture(ADDRESS_SET_ROW_PERMISSIONS, obj, options)
  }

  def retrieveRowPermissions(tableId: TableId, rowId: RowId): Future[Option[RowPermissions]] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    eventBus
      .sendFuture[JsonObject](ADDRESS_RETRIEVE_ROW_PERMISSIONS, obj, options)
      .map(value => {
        value match {
          case v if v.body().containsKey("value") => {
            val array = v.body().getValue("value").asInstanceOf[JsonArray]
            Some(RowPermissions(array))
          }
          case _ => None
        }
      })
      .recoverWith({
        case ex: ReplyException if ex.failureCode() == 404 => Future.successful(None)
        case ex => Future.failed(ex)
      })
  }

  def invalidateRowPermissions(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    eventBus.sendFuture(ADDRESS_INVALIDATE_ROW_PERMISSIONS, obj)
  }

  def setRowLevelAnnotations(
      tableId: TableId,
      rowId: RowId,
      rowLevelAnnotations: RowLevelAnnotations
  ): Future[_] = {
    val rowValue = Json.obj("value" -> rowLevelAnnotations.getJson)
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId).copy().mergeIn(rowValue)

    eventBus
      .sendFuture(ADDRESS_SET_ROW_LEVEL_ANNOTATIONS, obj, options)
  }

  def retrieveRowLevelAnnotations(tableId: TableId, rowId: RowId): Future[Option[RowLevelAnnotations]] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    eventBus
      .sendFuture[JsonObject](ADDRESS_RETRIEVE_ROW_LEVEL_ANNOTATIONS, obj, options)
      .map(value => {
        value match {
          case v if v.body().containsKey("value") => {
            val rawRowLevelAnnotations = v.body().getValue("value").asInstanceOf[JsonObject]
            val finalFlag = rawRowLevelAnnotations.getBoolean("final", false)
            val archivedFlag = rawRowLevelAnnotations.getBoolean("archived", false)
            Some(RowLevelAnnotations(finalFlag, archivedFlag))
          }
          case _ => None
        }
      })
      .recoverWith({
        case ex: ReplyException if ex.failureCode() == 404 => Future.successful(None)
        case ex => Future.failed(ex)
      })
  }

  def invalidateRowLevelAnnotations(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)

    eventBus.sendFuture(ADDRESS_INVALIDATE_ROW_LEVEL_ANNOTATIONS, obj)
  }

  def invalidateTableRowLevelAnnotations(tableId: TableId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId)
    eventBus.sendFuture(ADDRESS_INVALIDATE_TABLE_ROW_LEVEL_ANNOTATIONS, obj)
  }

  def invalidateCellValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId, "rowId" -> rowId)
    eventBus.sendFuture(ADDRESS_INVALIDATE_CELL, obj, options)
  }

  def invalidateColumn(tableId: TableId, columnId: ColumnId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "columnId" -> columnId)
    eventBus.sendFuture(ADDRESS_INVALIDATE_COLUMN, obj)
  }

  def invalidateRow(tableId: TableId, rowId: RowId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId, "rowId" -> rowId)
    eventBus.sendFuture(ADDRESS_INVALIDATE_ROW, obj)
  }

  def invalidateTable(tableId: TableId): Future[_] = {
    val obj = Json.obj("tableId" -> tableId)
    eventBus.sendFuture(ADDRESS_INVALIDATE_TABLE, obj)
  }

  def invalidateAll(): Future[_] = {
    eventBus.sendFuture(ADDRESS_INVALIDATE_ALL, Json.emptyObj())
  }
}
