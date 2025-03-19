package com.campudus.tableaux.verticles

import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{Service, ServiceType}
import com.campudus.tableaux.database.model.{ServiceModel, StructureModel, TableauxModel}
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}
import com.campudus.tableaux.verticles.EventClient._

import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.SQLConnection
import io.vertx.scala.core.MultiMap
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.ext.web.client.WebClient
import org.vertx.scala.core.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging

object MessagingVerticle {
  val ID_KEYS: Seq[String] = Seq("tableId", "columnId", "rowId")

  val EVENT_TYPE_TABLE_CREATED = "table_created"
  val EVENT_TYPE_TABLE_CHANGED = "table_changed"
  val EVENT_TYPE_TABLE_DELETED = "table_deleted"
  val EVENT_TYPE_ROW_CREATED = "row_created"
  val EVENT_TYPE_ROW_DELETED = "row_deleted"
  val EVENT_TYPE_ROW_ANNOTATION_CHANGED = "row_annotation_changed"
  val EVENT_TYPE_COLUMN_CREATED = "column_created"
  val EVENT_TYPE_COLUMN_CHANGED = "column_changed"
  val EVENT_TYPE_COLUMN_DELETED = "column_deleted"
  val EVENT_TYPE_CELL_ANNOTATION_CHANGED = "cell_annotation_changed"
  val EVENT_TYPE_CELL_CHANGED = "cell_changed"

  val eventTypes: Seq[String] = Seq(
    EVENT_TYPE_TABLE_CREATED,
    EVENT_TYPE_TABLE_CHANGED,
    EVENT_TYPE_TABLE_DELETED,
    EVENT_TYPE_ROW_CREATED,
    EVENT_TYPE_ROW_DELETED,
    EVENT_TYPE_ROW_ANNOTATION_CHANGED,
    EVENT_TYPE_COLUMN_CREATED,
    EVENT_TYPE_COLUMN_CHANGED,
    EVENT_TYPE_COLUMN_DELETED,
    EVENT_TYPE_CELL_CHANGED,
    EVENT_TYPE_CELL_ANNOTATION_CHANGED
  )

  def apply(tableauxConfig: TableauxConfig): MessagingVerticle = {
    new MessagingVerticle(tableauxConfig)
  }
}

class MessagingVerticle(tableauxConfig: TableauxConfig) extends ScalaVerticle with LazyLogging {

  import MessagingVerticle._

  private var tableauxModel: TableauxModel = _
  private var serviceModel: ServiceModel = _
  private var structureModel: StructureModel = _

  private var listeners: Map[String, Seq[Service]] = _

  private implicit var user: TableauxUser = _

  private lazy val eventBus = vertx.eventBus()

  private lazy val webClient: WebClient = WebClient.create(vertx)

  private def getServiceConfigValues(service: Service)
      : ((Option[(String, Integer, String)]), Option[String], MultiMap) = {
    val config = service.config

    val port = Option(config.getInteger("port"))
    val host = Option(config.getString("host"))
    val route = Option(config.getString("route"))

    val relativeUrl = (host, port, route) match {
      case (Some(h), Some(p), Some(r)) => Option(h, p, r)
      case _ => None
    }

    val absolutUrl = Option(config.getString("url"))
    val jsonHeaders = config.getJsonObject("headers", Json.emptyObj())
    val headers = Try(jsonHeaders.fieldNames().asScala
      .foldLeft(MultiMap.caseInsensitiveMultiMap()) { (acc, key) =>
        acc.add(key, jsonHeaders.getValue(key).toString())
        acc
      }).getOrElse(MultiMap.caseInsensitiveMultiMap())

    (relativeUrl, absolutUrl, headers)
  }

  private def retrieveListeners(): Future[Map[String, Seq[Service]]] = {
    for {
      services <- serviceModel.retrieveAll()
    } yield {
      val listeners = services.filter(service => {
        val hasRequiredConfigValues = getServiceConfigValues(service) match {
          case (_, Some(_), _) => true
          case (Some((_, _, _)), _, _) => true
          case _ => false
        }

        if (!hasRequiredConfigValues) {
          logger.warn(
            s"Service ${service.name} has invalid configuration values, either url or host, port and route must be set"
          )
        }
        service.serviceType.toString == ServiceType.LISTENER && hasRequiredConfigValues
      })

      val listenerMap = eventTypes.foldLeft[Map[String, Seq[Service]]](Map()) { (acc, eventType) =>
        {
          val listenersWithEventType = listeners.filter(service => {
            service.config.getJsonArray("events", Json.arr()).asScala.toSeq.contains(eventType)
          })

          acc + (eventType -> listenersWithEventType)
        }
      }

      listenerMap
    }
  }

  private def getApplicableListeners(
      eventType: String,
      maybeTableJson: Option[JsonObject] = None,
      maybeColumnJson: Option[JsonObject]
  ): Seq[Service] = {
    val listenersForEventType = listeners getOrElse (eventType, Seq())

    val getFilterRules: (String, String, JsonObject) => Seq[JsonObject] =
      (objectType: String, ruleType: String, scope: JsonObject) => {
        scope.getJsonObject(objectType, Json.obj()).getJsonArray(ruleType, Json.arr()).asScala.toSeq.map(obj => {
          obj.asInstanceOf[JsonObject]
        })
      }

    val filterFunction: Service => Boolean = (service: Service) => {
      val scope = service.scope

      val applyRules = (rules: Seq[JsonObject], objectJson: JsonObject) => {
        rules.exists(rule => {
          rule.fieldNames().asScala.toSeq.forall(key => {
            val str = rule.getValue(key, "").toString

            /*
             * If the first character in the regular expression is a + or *, don't parse regex
             * as this would result in an endless loop
             */
            if (Seq("+", "*").contains(str.substring(0, 1))) {
              false
            } else {
              val regex = str.r

              objectJson.getValue(key, "").toString match {
                case regex() => true
                case _ => false
              }
            }
          })
        })
      }
      maybeTableJson match {
        case Some(table) =>
          val tableIncludes = getFilterRules("tables", "includes", scope)
          val tableExcludes = getFilterRules("tables", "excludes", scope)
          val shouldIncludeTable = applyRules(tableIncludes, table) && !applyRules(tableExcludes, table)

          maybeColumnJson match {
            case Some(column) =>
              val columnIncludes = getFilterRules("columns", "includes", scope)
              val columnExcludes = getFilterRules("columns", "excludes", scope)
              val colIn = applyRules(columnIncludes, column)
              val colEx = !applyRules(columnExcludes, column)

              shouldIncludeTable && colIn && colEx
            case None =>
              shouldIncludeTable
          }
        case None => true
      }

    }

    val filtered = listenersForEventType.filter(filterFunction)

    filtered

  }

  override def startFuture(): Future[_] = {
    logger.info("start future")

    val isAuthorization: Boolean = !tableauxConfig.authConfig.isEmpty
    val roles = tableauxConfig.rolePermissions

    implicit val roleModel: RoleModel = RoleModel(roles, isAuthorization)

    val vertxAccess = new VertxAccess {
      override val vertx: Vertx = MessagingVerticle.this.vertx
    }

    val connection = SQLConnection(vertxAccess, tableauxConfig.databaseConfig)
    val dbConnection = DatabaseConnection(vertxAccess, connection)

    structureModel = StructureModel(dbConnection)
    user = TableauxUser("", roles.fieldNames().asScala.toSeq)
    tableauxModel = TableauxModel(dbConnection, structureModel, tableauxConfig)
    serviceModel = ServiceModel(dbConnection)

    for {
      listenersMap <- retrieveListeners()
      _ <- registerConsumers()
    } yield {
      listeners = listenersMap
    }

  }

  private def registerConsumers(): Future[Unit] = {
    def listen(address: String, handler: Message[JsonObject] => Future[Seq[Any]]): Future[Unit] = {
      eventBus.consumer(address, errorHandler(handler) _).completionFuture()
    }

    listen(ADDRESS_CELL_CHANGED, messageHandlerCellChanged)
    listen(ADDRESS_COLUMN_CREATED, messageHandlerColumnCreated())
    listen(ADDRESS_COLUMN_CHANGED, messageHandlerColumnChanged)
    listen(ADDRESS_COLUMN_DELETED, messageHandlerColumnDeleted)
    listen(ADDRESS_TABLE_CREATED, messageHandlerTableCreated())
    listen(ADDRESS_TABLE_CHANGED, messageHandlerTableChanged)
    listen(ADDRESS_TABLE_DELETED, messageHandlerTableDeleted)
    listen(ADDRESS_ROW_CREATED, messageHandlerRowCreated)
    listen(ADDRESS_ROW_DELETED, messageHandlerRowDeleted)
    listen(ADDRESS_ROW_ANNOTATION_CHANGED, messageHandlerRowAnnotationChanged)
    listen(ADDRESS_CELL_ANNOTATION_CHANGED, messageHandlerCellAnnotationChanged)

    eventBus.consumer(ADDRESS_SERVICES_CHANGED, messageHandlerUpdateListeners).completionFuture()
  }

  private def messageHandlerUpdateListeners(message: Message[JsonObject]): Unit = {
    for {
      listenersMap <- retrieveListeners()
    } yield {
      listeners = listenersMap
      message.reply("ok")
    }
  }

  private def messageHandlerCellChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    implicit val user: TableauxUser = TableauxUser("Messaging Verticle", Seq("dev"))

    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    val columnId = body.getLong("columnId").asInstanceOf[ColumnId]
    val rowId = body.getLong("rowId").asInstanceOf[RowId]

    for {
      table <- tableauxModel.retrieveTable(tableId, isInternalCall = true)
      column <- tableauxModel.retrieveColumn(table, columnId)
      cell <- tableauxModel.retrieveCell(table, column.id, rowId)
      dependentCells <- tableauxModel.retrieveDependentCells(table, rowId)

      dependentCellValues <- Future.sequence(dependentCells.flatMap {
        case (table, linkColumn, rowIds) =>
          rowIds.map(rowId => {
            tableauxModel.retrieveCell(table, linkColumn.id, rowId).map(data => {
              Json.obj("table" -> table.id, "column" -> linkColumn.id, "row" -> rowId).mergeIn(data.getJson)
            })
          })
      })

      data = Json.obj("cell" -> cell.getJson, "dependentCells" -> dependentCellValues)
      listeners = getApplicableListeners(EVENT_TYPE_CELL_CHANGED, Some(table.getJson), Some(column.getJson))

      listenerResponses <-
        sendMessage(listeners, createPayload(EVENT_TYPE_CELL_CHANGED, Some(tableId), Some(columnId), Some(rowId), data))
    } yield {
      listenerResponses
    }
  }

  def sendMessage(
      listeners: Seq[Service],
      payLoad: JsonObject
  ): Future[Seq[Any]] = {
    Future.sequence(listeners.map(listener => {
      val name = listener.name
      val (relativeUrl, absoluteUrl, headers) = getServiceConfigValues(listener)
      val baseErrorMsg = s"Send message failed, Service: $name,"

      absoluteUrl match {
        case Some(url) =>
          logger.info(s"Sending message, Service: $name, Absolut URL: $url")
          webClient.postAbs(url).putHeaders(headers).sendJsonObjectFuture(payLoad).recover { case err: Throwable =>
            logger.error(s"$baseErrorMsg Absolut URL: $url, Reason: ${err.getMessage}")
          }
        case None =>
          relativeUrl match {
            case Some((host, port, route)) =>
              logger.info(s"Sending message, Service: $name, Relative URL: $host:$port$route")
              webClient.post(port, host, route).putHeaders(headers).sendJsonObjectFuture(payLoad).recover {
                case err: Throwable =>
                  logger.error(s"$baseErrorMsg Relative URL: $host:$port$route, Reason: ${err.getMessage}")
              }
            case None =>
              Future.failed(new Exception(s"$baseErrorMsg neither absolute nor relative URL is set"))
          }
      }
    }))
  }

  private def createPayload(
      event: String,
      tableId: Option[TableId] = None,
      columnId: Option[ColumnId] = None,
      rowId: Option[RowId] = None,
      data: JsonObject = Json.obj()
  ): JsonObject = {
    val baseObj = Json.obj("event" -> event, "data" -> data)

    tableId match {
      case Some(id) => baseObj.put("tableId", id)
      case _ =>
    }

    columnId match {
      case Some(id) => baseObj.put("columnId", id)
      case _ =>
    }

    rowId match {
      case Some(id) => baseObj.put("rowId", id)
      case _ =>
    }

    baseObj
  }

  private def messageHandlerColumnCreated(
      eventType: String = EVENT_TYPE_COLUMN_CREATED
  )(
      message: Message[JsonObject]
  ): Future[Seq[Any]] = {
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]
    val columnId = body.getLong("columnId")

    for {
      table <- tableauxModel.retrieveTable(tableId, isInternalCall = true)
      column <- tableauxModel.retrieveColumn(table, columnId)
      listeners = getApplicableListeners(eventType, Some(table.getJson), Some(column.getJson))
      payload = createPayload(eventType, Some(tableId), Some(columnId), None, column.getJson)
      listenerResponses <- sendMessage(listeners, payload)
    } yield {
      listenerResponses
    }

  }

  private def messageHandlerTableCreated(
      eventType: String = EVENT_TYPE_TABLE_CREATED
  )(
      message: Message[JsonObject]
  ): Future[Seq[Any]] = {
    val body = message.body()
    val tableId = body.getLong("tableId").asInstanceOf[TableId]

    for {
      table <- tableauxModel.retrieveTable(tableId, isInternalCall = true)
      listeners = getApplicableListeners(eventType, Some(table.getJson), None)
      payload = createPayload(eventType, Some(tableId), None, None, table.getJson)
      listenerResponses <- sendMessage(listeners, payload)
    } yield {
      listenerResponses
    }

  }

  private def messageHandlerTableChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    messageHandlerTableCreated(EVENT_TYPE_TABLE_CHANGED)(message)
  }

  private def messageHandlerColumnChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    messageHandlerColumnCreated(EVENT_TYPE_COLUMN_CHANGED)(message)
  }

  private def getJsonValueAsOption[A](key: String, obj: JsonObject): Option[A] = {
    Try(obj.getValue(key).asInstanceOf[A]) match {
      // getValue returns null if the key doesn't exist ....
      case Success(null) => None
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  private def getId(key: String, obj: JsonObject): Option[Long] = getJsonValueAsOption(key, obj)

  private def getIds(body: JsonObject): (Option[Long], Option[Long], Option[Long]) = {
    ID_KEYS.map(key => getId(key, body)) match {
      case Seq(a, b, c) => (a, b, c)
    }
  }

  private def messageHandlerTableDeleted(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    val deletedTable = body.getJsonObject("table", Json.obj())
    val listeners = getApplicableListeners(EVENT_TYPE_TABLE_DELETED, Some(deletedTable), None)

    sendMessage(listeners, createPayload(EVENT_TYPE_TABLE_DELETED, tableId, columnId, rowId, deletedTable))
  }

  private def messageHandlerColumnDeleted(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    val deletedColumn = body.getJsonObject("column", Json.obj())

    for {
      table <- tableauxModel.retrieveTable(tableId.get, isInternalCall = true)
      listeners = getApplicableListeners(EVENT_TYPE_COLUMN_DELETED, Some(table.getJson), Some(deletedColumn))

      listenerResponses <-
        sendMessage(listeners, createPayload(EVENT_TYPE_COLUMN_DELETED, tableId, columnId, rowId, deletedColumn))
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerRowDeleted(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    for {
      table <- tableauxModel.retrieveTable(tableId.get, isInternalCall = true)
      listeners = getApplicableListeners(EVENT_TYPE_ROW_DELETED, Some(table.getJson), None)
      listenerResponses <- sendMessage(listeners, createPayload(EVENT_TYPE_ROW_DELETED, tableId, columnId, rowId))
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerRowCreated(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)

    for {
      table <- tableauxModel.retrieveTable(tableId.get, isInternalCall = true)
      row <- tableauxModel.retrieveRow(table, rowId.get)
      listeners = getApplicableListeners(EVENT_TYPE_ROW_CREATED, Some(table.getJson), None)

      listenerResponses <-
        sendMessage(listeners, createPayload(EVENT_TYPE_ROW_CREATED, tableId, columnId, rowId, row.getJson))
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerCellAnnotationChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)
    for {
      table <- tableauxModel.retrieveTable(tableId.get, isInternalCall = true)
      column <- tableauxModel.retrieveColumn(table, columnId.get)
      cellAnnotations <- tableauxModel.retrieveCellAnnotations(table, columnId.get, rowId.get)

      listeners =
        getApplicableListeners(EVENT_TYPE_CELL_ANNOTATION_CHANGED, Some(table.getJson), Some(column.getJson))

      listenerResponses <- sendMessage(
        listeners,
        createPayload(EVENT_TYPE_CELL_ANNOTATION_CHANGED, tableId, columnId, rowId, cellAnnotations.getJson)
      )
    } yield {
      listenerResponses
    }
  }

  private def messageHandlerRowAnnotationChanged(message: Message[JsonObject]): Future[Seq[Any]] = {
    val body = message.body()
    val (tableId, columnId, rowId) = getIds(body)

    for {
      table <- tableauxModel.retrieveTable(tableId.get, isInternalCall = true)
      row <- tableauxModel.retrieveRow(table, rowId.get)

      listeners =
        getApplicableListeners(EVENT_TYPE_ROW_ANNOTATION_CHANGED, Some(table.getJson), None)

      listenerResponses <- sendMessage(
        listeners,
        createPayload(EVENT_TYPE_ROW_ANNOTATION_CHANGED, tableId, columnId, rowId, row.getJson)
      )
    } yield {
      listenerResponses
    }
  }

  private def errorHandler(
      messageHandler: Message[JsonObject] => Future[Seq[Any]]
  )(message: Message[JsonObject]): Unit = {

    messageHandler(message).onComplete {
      case Success(_) =>
        message.reply("ok")
      case Failure(exception) =>
        logger.error(exception.getMessage)
        message.fail(500, exception.getMessage)
    }
  }
}
