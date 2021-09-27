package com.campudus.tableaux.router

import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.database.domain.{DisplayInfos, GenericTable, TableType}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{InvalidJsonException, TableauxConfig}
import io.vertx.scala.ext.web.handler.BodyHandler
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.collection.JavaConverters._

object StructureRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => StructureController): StructureRouter = {
    new StructureRouter(config, controllerCurry(config))
  }
}

class StructureRouter(override val config: TableauxConfig, val controller: StructureController) extends BaseRouter {

  private val column: String = s"/tables/$tableId/columns/$columnId"
  private val columns: String = s"/tables/$tableId/columns"

  private val table: String = s"/tables/$tableId"
  private val tables: String = "/tables"
  private val tableOrder: String = s"/tables/$tableId/order"

  private val group: String = s"/groups/$groupId"
  private val groups: String = "/groups"

  private val structure = "/structure"

  def route: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    router.get(tables).handler(retrieveTables)
    router.getWithRegex(table).handler(retrieveTable)
    router.getWithRegex(columns).handler(retrieveColumns)
    router.getWithRegex(column).handler(retrieveColumn)
    router.get(structure).handler(retrieveStructure)

    // DELETE
    router.deleteWithRegex(group).handler(deleteGroup)
    router.deleteWithRegex(table).handler(deleteTable)
    router.deleteWithRegex(column).handler(deleteColumn)

    // all following routes may require Json in the request body
    val bodyHandler = BodyHandler.create()
    router.post("/tables/*").handler(bodyHandler)
    router.patch("/tables/*").handler(bodyHandler)
    router.post("/groups/*").handler(bodyHandler)
    router.patch("/groups/*").handler(bodyHandler)

    // CREATE
    router.post(tables).handler(createTable)
    router.postWithRegex(columns).handler(createColumn)
    router.post(groups).handler(createGroup)

    // UPDATE
    router.postWithRegex(table).handler(updateTable)
    router.patchWithRegex(table).handler(updateTable)

    router.postWithRegex(tableOrder).handler(updateTableOrdering)
    router.patchWithRegex(tableOrder).handler(updateTableOrdering)

    router.postWithRegex(column).handler(updateColumn)
    router.patchWithRegex(column).handler(updateColumn)

    router.postWithRegex(group).handler(updateGroup)
    router.patchWithRegex(group).handler(updateGroup)

    router
  }

  private def retrieveStructure(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      controller.retrieveStructure()
    })
  }

  private def retrieveTables(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      controller.retrieveTables()
    })
  }

  private def retrieveTable(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveTable(tableId)
        }
      )
    }
  }

  private def retrieveColumns(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveColumns(tableId)
        }
      )
    }
  }

  private def retrieveColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          controller.retrieveColumn(tableId, columnId)
        }
      )
    }
  }

  private def createTable(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)

        val name = json.getString("name")
        val hidden = json.getBoolean("hidden", false).booleanValue()
        val displayInfos = DisplayInfos.fromJson(json)
        val tableType = TableType(json.getString("type", GenericTable.NAME))
        val attributes = Option(json.getJsonObject("attributes"))

        // if contains than user wants langtags to be set
        // but then langtags could be null so that's the second option
        //
        // langtags == null => global langtags
        // langtags == [] => [] table without langtags
        // langtags == ['de-DE', 'en-GB'] => table with two langtags
        //
        // {} => None => db: null
        // {langtags:null} => Some(None) => db: null
        // {langtags:['de-DE']} => Some(Seq('de-DE')) => db: ['de-DE']
        val langtags = booleanToValueOption(
          json.containsKey("langtags"),
          Option(json.getJsonArray("langtags")).map(_.asScala.map(_.toString).toSeq)
        )
        val tableGroupId = booleanToValueOption(json.containsKey("group"), json.getLong("group")).map(_.toLong)
        controller.createTable(name, hidden, langtags, displayInfos, tableType, tableGroupId, attributes)
      }
    )
  }

  private def createColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          controller.createColumns(tableId, toCreateColumnSeq(json))
        }
      )
    }
  }

  private def updateTable(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val name = Option(json.getString("name"))
          val hidden = Option(json.getBoolean("hidden")).map(_.booleanValue())
          val displayInfos = DisplayInfos.fromJson(json) match {
            case Nil => None
            case list => Some(list)
          }

          // if contains than user wants langtags to be set
          // but then langtags could be null so that's the second option
          //
          // langtags == null => global langtags
          // langtags == [] => [] table without langtags
          // langtags == ['de-DE', 'en-GB'] => table with two langtags
          //
          // {} => None => db: do nothing
          // {langtags:null} => Some(None) => db: overwrite with null
          // {langtags:['de-DE']} => Some(Seq('de-DE')) => db: overwrite with ['de-DE']
          val langtags = booleanToValueOption(
            json.containsKey("langtags"),
            Option(json.getJsonArray("langtags")).map(_.asScala.map(_.toString).toSeq)
          )

          val tableGroupId = booleanToValueOption(
            json.containsKey("group"),
            Option(json.getLong("group")).map(_.toLong)
          )

          controller.changeTable(tableId, name, hidden, langtags, displayInfos, tableGroupId)
        }
      )
    }
  }

  private def updateTableOrdering(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          val json = getJson(context)
          controller.changeTableOrder(tableId, toLocationType(json))
        }
      )
    }
  }

  private def updateColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val (optName, optOrd, optKind, optId, optDisplayInfos, optCountryCodes, optSeparator) =
            toColumnChanges(json)

          controller.changeColumn(tableId,
                                  columnId,
                                  optName,
                                  optOrd,
                                  optKind,
                                  optId,
                                  optDisplayInfos,
                                  optCountryCodes,
                                  optSeparator)
        }
      )
    }
  }

  private def createGroup(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        val json = getJson(context)
        val displayInfos = DisplayInfos.fromJson(json) match {
          case Nil =>
            throw InvalidJsonException("Either displayName or description or both must be defined!", "groups")
          case list => list
        }

        controller.createTableGroup(displayInfos)
      }
    )
  }

  private def updateGroup(context: RoutingContext): Unit = {
    for {
      groupId <- getGroupId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          val json = getJson(context)
          val displayInfos = DisplayInfos.fromJson(json) match {
            case list if list.isEmpty => None
            case list => Some(list)
          }

          controller.changeTableGroup(groupId, displayInfos)
        }
      )
    }
  }

  private def deleteGroup(context: RoutingContext): Unit = {
    for {
      groupId <- getGroupId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteTableGroup(groupId)
        }
      )
    }
  }

  private def deleteTable(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteTable(tableId)
        }
      )
    }
  }

  private def deleteColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteColumn(tableId, columnId)
        }
      )
    }
  }

  private def getGroupId(context: RoutingContext): Option[Long] = {
    getLongParam("groupId", context)
  }
}
