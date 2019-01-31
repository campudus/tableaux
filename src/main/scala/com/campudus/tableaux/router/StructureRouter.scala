package com.campudus.tableaux.router

import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.database.domain.{DisplayInfos, GenericTable, TableType}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.{InvalidJsonException, TableauxConfig}
import io.vertx.scala.ext.web.{Router, RoutingContext}

import scala.collection.JavaConverters._

object StructureRouter {

  def apply(config: TableauxConfig, controllerCurry: TableauxConfig => StructureController): StructureRouter = {
    new StructureRouter(config, controllerCurry(config))
  }
}

class StructureRouter(override val config: TableauxConfig, val controller: StructureController) extends BaseRouter {

  private val Column: String = s"""/tables/$TABLE_ID/columns/$COLUMN_ID"""
  private val Columns: String = s"""/tables/$TABLE_ID/columns"""

  private val Table: String = s"""/tables/$TABLE_ID"""
  private val Tables: String = "/tables"
  private val TableOrder: String = s"""/tables/$TABLE_ID/order"""

  private val Group: String = s"""/groups/$GROUP_ID"""
  private val Groups: String = "/groups"

  def route: Router = {
    val router = Router.router(vertx)

    // RETRIEVE
    router.get(Tables).handler(retrieveTables)
    router.getWithRegex(Table).handler(retrieveTable)
    router.getWithRegex(Columns).handler(retrieveColumns)
    router.getWithRegex(Column).handler(retrieveColumn)

    // CREATE
    router.post(Tables).handler(createTable)
    router.postWithRegex(Columns).handler(createColumn)
    router.post(Groups).handler(createGroup)

    // UPDATE
    router.postWithRegex(Table).handler(updateTable)
    router.patchWithRegex(Table).handler(updateTable)

    router.postWithRegex(TableOrder).handler(updateTableOrdering)
    router.patchWithRegex(TableOrder).handler(updateTableOrdering)

    router.postWithRegex(Column).handler(updateColumn)
    router.patchWithRegex(Column).handler(updateColumn)

    router.postWithRegex(Group).handler(updateGroup)
    router.patchWithRegex(Group).handler(updateGroup)

    // DELETE
    router.deleteWithRegex(Group).handler(deleteGroup)
    router.deleteWithRegex(Table).handler(deleteTable)
    router.deleteWithRegex(Column).handler(deleteColumn)

    router
  }

  def retrieveTables(context: RoutingContext): Unit = {
    sendReply(context, asyncGetReply {
      controller.retrieveTables()
    })
  }

  def retrieveTable(context: RoutingContext): Unit = {
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

  def retrieveColumns(context: RoutingContext): Unit = {
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

  def retrieveColumn(context: RoutingContext): Unit = {
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

  def createTable(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        for {
          json <- getJson(context)

          name = json.getString("name")
          hidden = json.getBoolean("hidden", false).booleanValue()
          displayInfos = DisplayInfos.fromJson(json)
          tableType = TableType(json.getString("type", GenericTable.NAME))

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
          langtags = booleanToValueOption(
            json.containsKey("langtags"),
            Option(json.getJsonArray("langtags")).map(_.asScala.map(_.toString).toSeq)
          )
          tableGroupId = booleanToValueOption(json.containsKey("group"), json.getLong("group")).map(_.toLong)
          created <- controller.createTable(name, hidden, langtags, displayInfos, tableType, tableGroupId)
        } yield created
      }
    )
  }

  def createColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            created <- controller.createColumns(tableId, toCreateColumnSeq(json))
          } yield created
        }
      )
    }
  }

  def updateTable(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)

            name = Option(json.getString("name"))
            hidden = Option(json.getBoolean("hidden")).map(_.booleanValue())
            displayInfos = DisplayInfos.fromJson(json) match {
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
            langtags = booleanToValueOption(
              json.containsKey("langtags"),
              Option(json.getJsonArray("langtags")).map(_.asScala.map(_.toString).toSeq)
            )

            tableGroupId = booleanToValueOption(json.containsKey("group"), Option(json.getLong("group")).map(_.toLong))
            updated <- controller.changeTable(tableId, name, hidden, langtags, displayInfos, tableGroupId)
          } yield updated
        }
      )
    }
  }

  def updateTableOrdering(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          for {
            json <- getJson(context)
            result <- controller.changeTableOrder(tableId.toLong, toLocationType(json))
          } yield result
        }
      )
    }
  }

  def updateColumn(context: RoutingContext): Unit = {
    for {
      tableId <- getTableId(context)
      columnId <- getColumnId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            (optName, optOrd, optKind, optId, optFrontendReadOnly, optDisplayInfos, optCountryCodes) = toColumnChanges(
              json)
            changed <- controller
              .changeColumn(tableId,
                            columnId,
                            optName,
                            optOrd,
                            optKind,
                            optId,
                            optFrontendReadOnly,
                            optDisplayInfos,
                            optCountryCodes)
          } yield changed
        }
      )
    }
  }

  def createGroup(context: RoutingContext): Unit = {
    sendReply(
      context,
      asyncGetReply {
        for {
          json <- getJson(context)
          displayInfos = DisplayInfos.fromJson(json) match {
            case Nil =>
              throw InvalidJsonException("Either displayName or description or both must be defined!", "groups")
            case list => list
          }

          created <- controller.createTableGroup(displayInfos)
        } yield created
      }
    )
  }

  def updateGroup(context: RoutingContext): Unit = {
    for {
      groupId <- getGroupId(context)
    } yield {
      sendReply(
        context,
        asyncGetReply {
          for {
            json <- getJson(context)
            displayInfos = DisplayInfos.fromJson(json) match {
              case list if list.isEmpty => None
              case list => Some(list)
            }

            changed <- controller.changeTableGroup(groupId, displayInfos)
          } yield changed
        }
      )
    }
  }

  def deleteGroup(context: RoutingContext): Unit = {
    for {
      groupId <- getGroupId(context)
    } yield {
      sendReply(
        context,
        asyncEmptyReply {
          controller.deleteTableGroup(groupId.toLong)
        }
      )
    }
  }

  def deleteTable(context: RoutingContext): Unit = {
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

  def deleteColumn(context: RoutingContext): Unit = {
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
}
