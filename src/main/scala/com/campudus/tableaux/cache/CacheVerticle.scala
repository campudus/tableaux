package com.campudus.tableaux.cache

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}

import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.eventbus.EventBus
import io.vertx.scala.core.eventbus.Message
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.collection.mutable
import scala.concurrent.Future
import scala.language.implicitConversions

import com.google.common.cache.CacheBuilder
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.TimeUnit
import scalacache._
import scalacache.guava._
import scalacache.modes.scalaFuture._

object CacheVerticle {

  /**
    * Default never expire
    */
  val DEFAULT_EXPIRE_AFTER_ACCESS: Long = -1L

  /**
    * Max. 100k cached values per column
    */
  val DEFAULT_MAXIMUM_SIZE: ColumnId = 100000L

  val NOT_FOUND_FAILURE: Int = 404
  val INVALID_MESSAGE: Int = 400

  val ADDRESS_SET_CELL: String = "cache.set.cell"
  val ADDRESS_SET_ROW_PERMISSIONS: String = "cache.set.row_permissions"
  val ADDRESS_RETRIEVE_CELL: String = "cache.retrieve.cell"
  val ADDRESS_RETRIEVE_ROW_PERMISSIONS: String = "cache.retrieve.row_permissions"

  val ADDRESS_INVALIDATE_CELL: String = "cache.invalidate.cell"
  val ADDRESS_INVALIDATE_COLUMN: String = "cache.invalidate.column"
  val ADDRESS_INVALIDATE_ROW: String = "cache.invalidate.row"
  val ADDRESS_INVALIDATE_TABLE: String = "cache.invalidate.table"
  val ADDRESS_INVALIDATE_ALL: String = "cache.invalidate.all"
  val ADDRESS_INVALIDATE_ROW_PERMISSIONS: String = "cache.invalidate.row_permissions"

  val TIMEOUT_AFTER_MILLISECONDS: Int = 400
}

class CacheVerticle extends ScalaVerticle with LazyLogging {

  import CacheVerticle._

  private lazy val eventBus = vertx.eventBus()

  type CellCaches = mutable.Map[(TableId, ColumnId), Cache[AnyRef]]
  type RowPermissionsCaches = mutable.Map[(TableId), Cache[AnyRef]]
  type Caches = Either[CellCaches, RowPermissionsCaches]

  private val cellCaches: CellCaches = mutable.Map.empty
  private val rowPermissionsCaches: RowPermissionsCaches = mutable.Map.empty

  override def startFuture(): Future[_] = {
    logger.info(
      s"CacheVerticle initialized: DEFAULT_MAXIMUM_SIZE: $DEFAULT_MAXIMUM_SIZE, DEFAULT_EXPIRE_AFTER_ACCESS: "
        + s"$DEFAULT_EXPIRE_AFTER_ACCESS TIMEOUT_AFTER_MILLISECONDS: $TIMEOUT_AFTER_MILLISECONDS"
    )
    registerOnEventBus()
  }

  private def registerHandler(
      eventBus: EventBus,
      address: String,
      handler: Handler[Message[JsonObject]]
  ): Future[Unit] = eventBus.localConsumer(address, handler).completionFuture()

  private def registerOnEventBus(): Future[_] = {
    Future.sequence(
      Seq(
        registerHandler(eventBus, ADDRESS_SET_CELL, messageHandlerSetCell),
        registerHandler(eventBus, ADDRESS_SET_CELL, messageHandlerSetCell),
        registerHandler(eventBus, ADDRESS_RETRIEVE_CELL, messageHandlerRetrieveCell),
        registerHandler(eventBus, ADDRESS_SET_ROW_PERMISSIONS, messageHandlerSetRowPermissions),
        registerHandler(eventBus, ADDRESS_RETRIEVE_ROW_PERMISSIONS, messageHandlerRetrieveRowPermissions),
        registerHandler(eventBus, ADDRESS_INVALIDATE_CELL, messageHandlerInvalidateCell),
        registerHandler(eventBus, ADDRESS_INVALIDATE_COLUMN, messageHandlerInvalidateColumn),
        registerHandler(eventBus, ADDRESS_INVALIDATE_ROW, messageHandlerInvalidateRow),
        registerHandler(eventBus, ADDRESS_INVALIDATE_TABLE, messageHandlerInvalidateTable),
        registerHandler(eventBus, ADDRESS_INVALIDATE_ALL, messageHandlerInvalidateAll),
        registerHandler(eventBus, ADDRESS_INVALIDATE_ROW_PERMISSIONS, messageHandlerInvalidateRowPermissions)
      )
    )
  }

  private def createCache() = {
    val builder = CacheBuilder.newBuilder()

    val expireAfterAccess = config.getLong("expireAfterAccess", DEFAULT_EXPIRE_AFTER_ACCESS).toLong
    if (expireAfterAccess > 0) {
      builder.expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
    }

    val maximumSize = config.getLong("maximumSize", DEFAULT_MAXIMUM_SIZE).toLong
    if (maximumSize > 0) {
      builder.maximumSize(maximumSize)
    }

    builder.recordStats()
    builder.build[String, Entry[AnyRef]]
  }

  private def getCellCache(tableId: TableId, columnId: ColumnId): Cache[AnyRef] = {
    cellCaches.get(tableId, columnId) match {
      case Some(cache) => cache
      case None =>
        val cache: Cache[AnyRef] = GuavaCache(createCache())
        cellCaches.put((tableId, columnId), cache)
        cache
    }
  }

  private def getRowPermissionsCache(tableId: TableId): Cache[AnyRef] = {
    rowPermissionsCaches.get(tableId) match {
      case Some(cache) => cache
      case None =>
        val cache: Cache[AnyRef] = GuavaCache(createCache())
        rowPermissionsCaches.put((tableId), cache)
        cache
    }
  }

  private def removeCache(tableId: TableId, columnId: ColumnId): Unit = cellCaches.remove((tableId, columnId))

  private def extractTableColumnRow(obj: JsonObject): Option[(TableId, ColumnId, RowId)] = {
    for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      columnId <- Option(obj.getLong("columnId")).map(_.toLong)
      rowId <- Option(obj.getLong("rowId")).map(_.toLong)
    } yield (tableId, columnId, rowId)
  }

  private def extractTableRow(obj: JsonObject): Option[(TableId, RowId)] = {
    for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      rowId <- Option(obj.getLong("rowId")).map(_.toLong)
    } yield (tableId, rowId)
  }

  private def messageHandlerSetCell(message: Message[JsonObject]): Unit = {
    val obj = message.body()
    val value = obj.getValue("value")

    extractTableColumnRow(obj) match {
      case Some((tableId, columnId, rowId)) =>
        implicit val scalaCache: Cache[AnyRef] = getCellCache(tableId, columnId)
        put(rowId)(value).map(_ => replyJson(message, tableId, columnId, rowId))

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def messageHandlerRetrieveCell(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    extractTableColumnRow(obj) match {
      case Some((tableId, columnId, rowId)) =>
        implicit val scalaCache: Cache[AnyRef] = getCellCache(tableId, columnId)

        get(rowId)
          .map({
            case Some(value) =>
              val reply = Json.obj(
                "tableId" -> tableId,
                "columnId" -> columnId,
                "rowId" -> rowId,
                "value" -> value
              )

              message.reply(reply)
            case None =>
              logger.debug(s"messageHandlerRetrieveCell $tableId, $columnId, $rowId not found")
              message.fail(NOT_FOUND_FAILURE, "Not found")
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def messageHandlerSetRowPermissions(message: Message[JsonObject]): Unit = {
    val obj = message.body()
    val value = obj.getValue("value")

    extractTableRow(obj) match {
      case Some((tableId, rowId)) => {
        implicit val scalaCache: Cache[AnyRef] = getRowPermissionsCache(tableId)
        logger.info(s"messageHandlerSetRowPermissions $obj $tableId $rowId $value")
        put(rowId)(value).map(_ => replyJson(message, tableId, rowId))
      }
      case None => {
        logger.error("Message invalid: Fields (tableId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, rowId) should be a Long")
      }
    }
  }

  private def messageHandlerRetrieveRowPermissions(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    extractTableRow(obj) match {
      case Some((tableId, rowId)) =>
        implicit val scalaCache: Cache[AnyRef] = getRowPermissionsCache(tableId)

        get(rowId).map({
          case Some(value) => {
            val reply = Json.obj(
              "tableId" -> tableId,
              "rowId" -> rowId,
              "value" -> value
            )

            message.reply(reply)
          }
          case None => {
            logger.debug(s"messageHandlerRetrieveRow $tableId, $rowId not found")
            message.fail(NOT_FOUND_FAILURE, "Not found")
          }
        })

      case None =>
        logger.error("Message invalid: Fields (tableId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, rowId) should be a Long")
    }
  }

  private def messageHandlerInvalidateCell(message: Message[JsonObject]): Unit = {
    extractTableColumnRow(message.body()) match {
      case Some((tableId, columnId, rowId)) =>
        // invalidate cell
        implicit val scalaCache: Cache[AnyRef] = getCellCache(tableId, columnId)

        remove(rowId)
          .map(_ => replyJson(message, tableId, columnId, rowId))

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def replyJson(message: Message[JsonObject], tableId: TableId, columnId: ColumnId, rowId: RowId): Unit = {
    val reply = Json.obj("tableId" -> tableId, "columnId" -> columnId, "rowId" -> rowId)
    message.reply(reply)
  }

  private def replyJson(message: Message[JsonObject], tableId: TableId, rowId: ColumnId): Unit = {
    val reply = Json.obj("tableId" -> tableId, "rowId" -> rowId)
    message.reply(reply)
  }

  private def messageHandlerInvalidateColumn(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      columnId <- Option(obj.getLong("columnId")).map(_.toLong)
    } yield (tableId, columnId)) match {
      case Some((tableId, columnId)) =>
        // invalidate column
        implicit val scalaCache: Cache[AnyRef] = getCellCache(tableId, columnId)

        removeAll()
          .map(_ => {
            removeCache(tableId, columnId)
            val reply = Json.obj("tableId" -> tableId, "columnId" -> columnId)
            message.reply(reply)
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId) should be a Long")
    }
  }

  private def filterCellCaches(tableId: TableId) = {
    // invalidate table
    cellCaches.filterKeys({
      case (cachedTableId, _) =>
        cachedTableId == tableId
    }).values
  }

  private def filterRowPermissionCaches(tableId: TableId) = {
    // invalidate table
    rowPermissionsCaches.filterKeys({
      case (cachedTableId) =>
        cachedTableId == tableId
    }).values
  }

  private def messageHandlerInvalidateRow(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (extractTableRow(obj)) match {
      case Some((tableId, rowId)) =>
        Future
          .sequence(
            filterCellCaches(tableId)
              .map(implicit cache => {
                remove(rowId)
              })
          )
          .map(_ => {
            val reply = Json.obj("tableId" -> tableId, "rowId" -> rowId)
            message.reply(reply)
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, rowId) should be a Long")
    }
  }

  private def messageHandlerInvalidateTable(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
    } yield tableId) match {
      case Some(tableId) =>
        Future
          .sequence(
            filterCellCaches(tableId)
              .map(implicit cache => {
                removeAll()
              })
          )
          .map(_ => {
            val reply = Json.obj("tableId" -> tableId)
            message.reply(reply)
          })

      case None =>
        logger.error("Message invalid: Fields (tableId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId) should be a Long")
    }
  }

  private def messageHandlerInvalidateAll(message: Message[JsonObject]): Unit = {
    Future
      .sequence(cellCaches.map({
        case ((tableId, columnId), cache) =>
          implicit val implicitCache: Cache[AnyRef] = implicitly(cache)

          removeAll().map(_ => removeCache(tableId, columnId))
      }))
      .onComplete(_ => {
        cellCaches.clear()
        message.reply(Json.emptyObj())
      })
  }

  private def messageHandlerInvalidateRowPermissions(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (extractTableRow(obj)) match {
      case Some((tableId, rowId)) =>
        Future
          .sequence(
            filterRowPermissionCaches(tableId)
              .map(implicit cache => {
                remove(rowId)
              })
          )
          .map(_ => {
            val reply = Json.obj("tableId" -> tableId, "rowId" -> rowId)
            message.reply(reply)
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, rowId) should be a Long")
    }
  }
}
