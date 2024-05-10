package com.campudus.tableaux.cache

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}

import io.vertx.lang.scala.ScalaVerticle
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

  val ADDRESS_SET: String = "cache.set"
  val ADDRESS_RETRIEVE: String = "cache.retrieve"

  val ADDRESS_INVALIDATE_CELL: String = "cache.invalidate.cell"
  val ADDRESS_INVALIDATE_COLUMN: String = "cache.invalidate.column"
  val ADDRESS_INVALIDATE_ROW: String = "cache.invalidate.row"
  val ADDRESS_INVALIDATE_TABLE: String = "cache.invalidate.table"
  val ADDRESS_INVALIDATE_ALL: String = "cache.invalidate.all"

  val TIMEOUT_AFTER_MILLISECONDS: Int = 400
}

class CacheVerticle extends ScalaVerticle with LazyLogging {

  import CacheVerticle._

  private lazy val eventBus = vertx.eventBus()

  private val caches: mutable.Map[(TableId, ColumnId), Cache[AnyRef]] = mutable.Map.empty

  override def startFuture(): Future[_] = {
    logger.info(
      s"CacheVerticle initialized: DEFAULT_MAXIMUM_SIZE: $DEFAULT_MAXIMUM_SIZE, DEFAULT_EXPIRE_AFTER_ACCESS: "
        + s"$DEFAULT_EXPIRE_AFTER_ACCESS TIMEOUT_AFTER_MILLISECONDS: $TIMEOUT_AFTER_MILLISECONDS"
    )
    registerOnEventBus()
  }

  private def registerOnEventBus(): Future[_] = {
    Future.sequence(
      Seq(
        eventBus.localConsumer(ADDRESS_SET, messageHandlerSet).completionFuture(),
        eventBus.localConsumer(ADDRESS_RETRIEVE, messageHandlerRetrieve).completionFuture(),
        eventBus.localConsumer(ADDRESS_INVALIDATE_CELL, messageHandlerInvalidateCell).completionFuture(),
        eventBus.localConsumer(ADDRESS_INVALIDATE_COLUMN, messageHandlerInvalidateColumn).completionFuture(),
        eventBus.localConsumer(ADDRESS_INVALIDATE_ROW, messageHandlerInvalidateRow).completionFuture(),
        eventBus.localConsumer(ADDRESS_INVALIDATE_TABLE, messageHandlerInvalidateTable).completionFuture(),
        eventBus.localConsumer(ADDRESS_INVALIDATE_ALL, messageHandlerInvalidateAll).completionFuture()
      )
    )
  }

  private def getCache(tableId: TableId, columnId: ColumnId): Cache[AnyRef] = {

    def createCache() = {
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

    caches.get(tableId, columnId) match {
      case Some(cache) => cache
      case None =>
        val cache: Cache[AnyRef] = GuavaCache(createCache())
        caches.put((tableId, columnId), cache)
        cache
    }
  }

  private def removeCache(tableId: TableId, columnId: ColumnId): Unit = caches.remove((tableId, columnId))

  private def messageHandlerSet(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    val value = obj.getValue("value")

    extractTableColumnRow(obj) match {
      case Some((tableId, columnId, rowId)) =>
        implicit val scalaCache: Cache[AnyRef] = getCache(tableId, columnId)
        put(rowId)(value)
          .map(_ => replyJson(message, tableId, columnId, rowId))

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def extractTableColumnRow(obj: JsonObject): Option[(ColumnId, ColumnId, ColumnId)] = {
    for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      columnId <- Option(obj.getLong("columnId")).map(_.toLong)
      rowId <- Option(obj.getLong("rowId")).map(_.toLong)
    } yield (tableId, columnId, rowId)
  }

  private def messageHandlerRetrieve(message: Message[JsonObject]): Unit = {
    extractTableColumnRow(message.body()) match {
      case Some((tableId, columnId, rowId)) =>
        implicit val scalaCache: Cache[AnyRef] = getCache(tableId, columnId)

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
              logger.debug(s"messageHandlerRetrieve $tableId, $columnId, $rowId not found")
              message.fail(NOT_FOUND_FAILURE, "Not found")
          })

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def messageHandlerInvalidateCell(message: Message[JsonObject]): Unit = {
    extractTableColumnRow(message.body()) match {
      case Some((tableId, columnId, rowId)) =>
        // invalidate cell
        implicit val scalaCache: Cache[AnyRef] = getCache(tableId, columnId)

        remove(rowId)
          .map(_ => replyJson(message, tableId, columnId, rowId))

      case None =>
        logger.error("Message invalid: Fields (tableId, columnId, rowId) should be a Long")
        message.fail(INVALID_MESSAGE, "Message invalid: Fields (tableId, columnId, rowId) should be a Long")
    }
  }

  private def replyJson(message: Message[JsonObject], tableId: ColumnId, columnId: ColumnId, rowId: ColumnId): Unit = {
    val reply = Json.obj("tableId" -> tableId, "columnId" -> columnId, "rowId" -> rowId)
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
        implicit val scalaCache: Cache[AnyRef] = getCache(tableId, columnId)

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

  private def filterScalaCaches(tableId: TableId) = {

    // invalidate table
    caches
      .filterKeys({
        case (cachedTableId, _) =>
          cachedTableId == tableId
      })
      .values
  }

  private def messageHandlerInvalidateRow(message: Message[JsonObject]): Unit = {
    val obj = message.body()

    (for {
      tableId <- Option(obj.getLong("tableId")).map(_.toLong)
      rowId <- Option(obj.getLong("rowId")).map(_.toLong)
    } yield (tableId, rowId)) match {
      case Some((tableId, rowId)) =>
        Future
          .sequence(
            filterScalaCaches(tableId)
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
            filterScalaCaches(tableId)
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
      .sequence(caches.map({
        case ((tableId, columnId), cache) =>
          implicit val implicitCache: Cache[AnyRef] = implicitly(cache)

          removeAll().map(_ => removeCache(tableId, columnId))
      }))
      .onComplete(_ => {
        caches.clear()
        message.reply(Json.emptyObj())
      })
  }
}
