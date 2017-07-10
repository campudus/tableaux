package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.{ForbiddenException, TableauxConfig}

import scala.concurrent.Future

object StructureController {

  def apply(config: TableauxConfig, repository: StructureModel): StructureController = {
    new StructureController(config, repository)
  }
}

class StructureController(override val config: TableauxConfig, override protected val repository: StructureModel)
    extends Controller[StructureModel] {

  val tableStruc = repository.tableStruc
  val columnStruc = repository.columnStruc
  val tableGroupStruc = repository.tableGroupStruc

  def retrieveTable(tableId: TableId): Future[Table] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTable $tableId")

    tableStruc.retrieve(tableId)
  }

  def retrieveTables(): Future[TableSeq] = {
    logger.info(s"retrieveTables")

    tableStruc.retrieveAll().map(TableSeq)
  }

  def createColumns(tableId: TableId, columns: Seq[CreateColumn]): Future[ColumnSeq] = {
    checkArguments(greaterZero(tableId), nonEmpty(columns, "columns"))
    logger.info(s"createColumns $tableId columns $columns")

    for {
      table <- retrieveTable(tableId)
      created <- table.tableType match {
        case GenericTable => columnStruc.createColumns(table, columns)
        case SettingsTable => Future.failed(ForbiddenException("can't add a column to a settings table", "column"))
      }

      retrieved <- Future.sequence(created.map(c => retrieveColumn(c.table.id, c.id)))
      sorted = retrieved.sortBy(_.ordering)
    } yield {
      ColumnSeq(sorted)
    }
  }

  def retrieveColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"))
    logger.info(s"retrieveColumn $tableId $columnId")

    for {
      table <- tableStruc.retrieve(tableId)
      column <- columnStruc.retrieve(table, columnId)
    } yield column
  }

  def retrieveColumns(tableId: TableId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveColumns $tableId")

    for {
      table <- tableStruc.retrieve(tableId)
      columns <- columnStruc.retrieveAll(table)
    } yield ColumnSeq(columns)
  }

  def createTable(
      tableName: String,
      hidden: Boolean,
      langtags: Option[Option[Seq[String]]],
      displayInfos: Seq[DisplayInfo],
      tableType: TableType,
      tableGroupId: Option[TableGroupId]
  ): Future[Table] = {
    checkArguments(notNull(tableName, "name"))
    logger.info(s"createTable $tableName $hidden $langtags $displayInfos $tableType $tableGroupId")

    tableType match {
      case SettingsTable => createSettingsTable(tableName, hidden, langtags, displayInfos, tableGroupId)
      case _ => createGenericTable(tableName, hidden, langtags, displayInfos, tableGroupId)
    }
  }

  def createGenericTable(
      tableName: String,
      hidden: Boolean,
      langtags: Option[Option[Seq[String]]],
      displayInfos: Seq[DisplayInfo],
      tableGroupId: Option[TableGroupId]
  ): Future[Table] = {
    for {
      created <- tableStruc.create(tableName, hidden, langtags, displayInfos, GenericTable, tableGroupId)
      retrieved <- tableStruc.retrieve(created.id)
    } yield retrieved
  }

  def createSettingsTable(
      tableName: String,
      hidden: Boolean,
      langtags: Option[Option[Seq[String]]],
      displayInfos: Seq[DisplayInfo],
      tableGroupId: Option[TableGroupId]
  ): Future[Table] = {
    for {
      created <- tableStruc.create(tableName, hidden, langtags, displayInfos, SettingsTable, tableGroupId)

      _ <- columnStruc.createColumn(
        created,
        CreateSimpleColumn("key", None, ShortTextType, LanguageNeutral, identifier = true, Seq.empty))
      _ <- columnStruc.createColumn(
        created,
        CreateSimpleColumn("displayKey",
                           None,
                           ShortTextType,
                           MultiLanguage,
                           identifier = false,
                           Seq(
                             NameOnly("de", "Bezeichnung"),
                             NameOnly("en", "Identifier")
                           ))
      )
      _ <- columnStruc.createColumn(created,
                                    CreateSimpleColumn("value",
                                                       None,
                                                       TextType,
                                                       MultiLanguage,
                                                       identifier = false,
                                                       Seq(
                                                         NameOnly("de", "Inhalt"),
                                                         NameOnly("en", "Value")
                                                       )))
      _ <- columnStruc.createColumn(created,
                                    CreateAttachmentColumn("attachment",
                                                           None,
                                                           identifier = false,
                                                           Seq(
                                                             NameOnly("de", "Anhang"),
                                                             NameOnly("en", "Attachment")
                                                           )))

      retrieved <- tableStruc.retrieve(created.id)
    } yield retrieved
  }

  def deleteTable(tableId: TableId): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"deleteTable $tableId")

    for {
      table <- tableStruc.retrieve(tableId)
      columns <- columnStruc.retrieveAll(table)

      // only delete special column before deleting table;
      // e.g. link column are based on simple columns
      _ <- columns.foldLeft(Future.successful(())) {
        case (future, column: LinkColumn) =>
          future.flatMap(_ => columnStruc.deleteLinkBothDirections(table, column.id))
        case (future, column: AttachmentColumn) =>
          future.flatMap(_ => columnStruc.delete(table, column.id))
        case (future, _) =>
          future.flatMap(_ => Future.successful(()))
      }

      _ <- tableStruc.delete(tableId)

      _ <- Future.sequence(columns.map({ column =>
        {
          CacheClient(this).invalidateColumn(tableId, column.id)
        }
      }))
    } yield EmptyObject()
  }

  def deleteColumn(tableId: TableId, columnId: ColumnId): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"deleteColumn $tableId $columnId")

    for {
      table <- tableStruc.retrieve(tableId)
      _ <- table.tableType match {
        case GenericTable => columnStruc.delete(table, columnId)
        case SettingsTable =>
          Future.failed(ForbiddenException("can't delete a column from a settings table", "column"))
      }

      _ <- CacheClient(this).invalidateColumn(tableId, columnId)
    } yield EmptyObject()
  }

  def changeTable(
      tableId: TableId,
      tableName: Option[String],
      hidden: Option[Boolean],
      langtags: Option[Option[Seq[String]]],
      displayInfos: Option[Seq[DisplayInfo]],
      tableGroupId: Option[Option[TableGroupId]]
  ): Future[Table] = {
    checkArguments(greaterZero(tableId),
                   isDefined(Seq(tableName, hidden, langtags, displayInfos, tableGroupId),
                             "name, hidden, langtags, displayName, description, group"))
    logger.info(s"changeTable $tableId $tableName $hidden $langtags $displayInfos $tableGroupId")

    for {
      _ <- tableStruc.change(tableId, tableName, hidden, langtags, displayInfos, tableGroupId)
      table <- tableStruc.retrieve(tableId)
    } yield {
      logger.info(s"retrieved table after change $table")
      table
    }
  }

  def changeTableOrder(tableId: TableId, locationType: LocationType): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"changeTableOrder $tableId $locationType")

    for {
      _ <- tableStruc.changeOrder(tableId, locationType)
    } yield EmptyObject()
  }

  def changeColumn(tableId: TableId,
                   columnId: ColumnId,
                   columnName: Option[String],
                   ordering: Option[Ordering],
                   kind: Option[TableauxDbType],
                   identifier: Option[Boolean],
                   displayInfos: Option[Seq[DisplayInfo]],
                   countryCodes: Option[Seq[String]]): Future[ColumnType[_]] = {
    checkArguments(greaterZero(tableId),
                   greaterZero(columnId),
                   isDefined(Seq(columnName, ordering, kind, identifier, displayInfos, countryCodes)))
    logger.info(
      s"changeColumn $tableId $columnId name=$columnName ordering=$ordering kind=$kind identifier=$identifier displayInfos=$displayInfos, countryCodes=$countryCodes")

    for {
      table <- tableStruc.retrieve(tableId)
      changed <- table.tableType match {
        case GenericTable =>
          columnStruc.change(table, columnId, columnName, ordering, kind, identifier, displayInfos, countryCodes)
        case SettingsTable => Future.failed(ForbiddenException("can't change a column of a settings table", "column"))
      }

      _ <- CacheClient(this).invalidateColumn(tableId, columnId)
    } yield changed
  }

  def createTableGroup(displayInfos: Seq[DisplayInfo]): Future[TableGroup] = {
    checkArguments(nonEmpty(displayInfos, "displayName or description"))
    logger.info(s"createTableGroup $displayInfos")

    for {
      tableGroup <- tableGroupStruc.create(displayInfos)
    } yield tableGroup
  }

  def changeTableGroup(tableGroupId: TableGroupId, displayInfos: Option[Seq[DisplayInfo]]): Future[TableGroup] = {
    checkArguments(greaterZero(tableGroupId), isDefined(displayInfos, "displayName or description"))
    logger.info(s"changeTableGroup $tableGroupId $displayInfos")

    for {
      _ <- tableGroupStruc.change(tableGroupId, displayInfos)
      tableGroup <- tableGroupStruc.retrieve(tableGroupId)
    } yield tableGroup
  }

  def deleteTableGroup(tableGroupId: TableGroupId): Future[EmptyObject] = {
    checkArguments(greaterZero(tableGroupId))
    logger.info(s"deleteTableGroup $tableGroupId")

    for {
      _ <- tableGroupStruc.delete(tableGroupId)
    } yield EmptyObject()
  }
}
