package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{
  AttachmentColumn,
  CreateAttachmentColumn,
  EmptyObject,
  LinkColumn,
  NameOnly,
  _
}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.structure.{CachedColumnModel, TableGroupModel, TableModel}
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.{ForbiddenException, RequestContext, TableauxConfig}

import scala.concurrent.Future

object StructureController {

  def apply(config: TableauxConfig, repository: StructureModel, roleModel: RoleModel)(
      implicit requestContext: RequestContext
  ): StructureController = {
    new StructureController(config, repository, roleModel)
  }
}

class StructureController(
    override val config: TableauxConfig,
    override protected val repository: StructureModel,
    implicit protected val roleModel: RoleModel
)(implicit requestContext: RequestContext)
    extends Controller[StructureModel] {

  val tableStruc: TableModel = repository.tableStruc
  val columnStruc: CachedColumnModel = repository.columnStruc
  val tableGroupStruc: TableGroupModel = repository.tableGroupStruc

  def retrieveTable(tableId: TableId): Future[Table] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTable $tableId")

    for {
      table <- tableStruc.retrieve(tableId)
    } yield table
  }

  def retrieveTables(): Future[TableSeq] = {
    logger.info(s"retrieveTables")

    for {
      tableSeq: Seq[Table] <- tableStruc.retrieveAll(isInternalCall = false)
    } yield {
      TableSeq(tableSeq)
    }
  }

  def createColumns(tableId: TableId, columns: Seq[CreateColumn]): Future[ColumnSeq] = {
    checkArguments(greaterZero(tableId), nonEmpty(columns, "columns"))
    logger.info(s"createColumns $tableId columns $columns")

    for {
      table <- retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(Create, ScopeColumn, ComparisonObjects(table))
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
      _ <- roleModel.checkAuthorization(View, ScopeColumn, ComparisonObjects(table, column))
    } yield column
  }

  def retrieveColumns(tableId: TableId, isInternalCall: Boolean = false): Future[ColumnSeq] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveColumns $tableId")

    for {
      table <- tableStruc.retrieve(tableId)
      columns <- columnStruc.retrieveAll(table)
    } yield {
      val filteredColumns: Seq[ColumnType[_]] =
        roleModel.filterDomainObjects[ColumnType[_]](ScopeColumn, columns, ComparisonObjects(table), isInternalCall)
      ColumnSeq(filteredColumns)
    }
  }

  def retrieveStructure(): Future[TablesStructure] = {
    type ColumnSeq = Seq[ColumnType[_]]
    type IdColumnTuple = (TableId, ColumnSeq)

    def collectColumns(table: Table): Future[IdColumnTuple] = {
      columnStruc
        .retrieveAll(table)
        .map(columnSeq => (table.id -> columnSeq))
    }

    def toColumnMap =
      (
          mapAccum: Map[TableId, ColumnSeq],
          next: IdColumnTuple
      ) =>
        next match {
          case (id, columns) => mapAccum + (id -> columns)
          case _ => mapAccum
      }

    val emptyMap = Map[TableId, ColumnSeq]()

    for {
      tables <- tableStruc.retrieveAll(isInternalCall = false)
      columns <- Future.sequence(tables.map(collectColumns))
    } yield {
      val columnMap = columns.foldLeft(emptyMap)(toColumnMap)
      TablesStructure(tables, columnMap)
    }
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
      _ <- roleModel.checkAuthorization(Create, ScopeTable)
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
      _ <- roleModel.checkAuthorization(Create, ScopeTable)
      created <- tableStruc.create(tableName, hidden, langtags, displayInfos, SettingsTable, tableGroupId)

      _ <- columnStruc.createColumn(
        created,
        CreateSimpleColumn("key", None, ShortTextType, LanguageNeutral, identifier = true, Seq.empty)
      )
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
      _ <- columnStruc.createColumn(
        created,
        CreateSimpleColumn("value",
                           None,
                           TextType,
                           MultiLanguage,
                           identifier = false,
                           Seq(
                             NameOnly("de", "Inhalt"),
                             NameOnly("en", "Value")
                           ))
      )
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
      _ <- roleModel.checkAuthorization(Delete, ScopeTable, ComparisonObjects(table))
      columns <- columnStruc.retrieveAll(table)

      // only delete special column before deleting table;
      // e.g. link column are based on simple columns
      _ <- columns.foldLeft(Future.successful(())) {
        case (future, column: LinkColumn) =>
          future.flatMap(_ => columnStruc.deleteLinkBothDirections(table, column.id))
        case (future, column: AttachmentColumn) =>
          future.flatMap(_ => columnStruc.delete(table, column.id, bothDirections = false, checkForLastColumn = false))
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
      column <- columnStruc.retrieve(table, columnId)
      _ <- roleModel.checkAuthorization(Delete, ScopeColumn, ComparisonObjects(table, column))
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

    val structureProperties: Seq[Option[Any]] = Seq(tableName, hidden, langtags, tableGroupId)
    val isAtLeastOneStructureProperty: Boolean = structureProperties.exists(_.isDefined)

    logger.info(s"changeTable $tableId $tableName $hidden $langtags $displayInfos $tableGroupId")

    for {
      table <- tableStruc.retrieve(tableId)
      _ <- if (isAtLeastOneStructureProperty) {
        roleModel.checkAuthorization(EditStructureProperty, ScopeTable, ComparisonObjects(table))
      } else {
        roleModel.checkAuthorization(EditDisplayProperty, ScopeTable, ComparisonObjects(table))
      }
      _ <- tableStruc.change(tableId, tableName, hidden, langtags, displayInfos, tableGroupId)
      changedTable <- tableStruc.retrieve(tableId)
    } yield {
      logger.info(s"retrieved table after change $changedTable")
      changedTable
    }
  }

  def changeTableOrder(tableId: TableId, locationType: LocationType): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"changeTableOrder $tableId $locationType")

    for {
      table <- tableStruc.retrieve(tableId)
      _ <- roleModel.checkAuthorization(EditDisplayProperty, ScopeTable, ComparisonObjects(table))
      _ <- tableStruc.changeOrder(tableId, locationType)
    } yield EmptyObject()
  }

  def changeColumn(
      tableId: TableId,
      columnId: ColumnId,
      columnName: Option[String],
      ordering: Option[Ordering],
      kind: Option[TableauxDbType],
      identifier: Option[Boolean],
      displayInfos: Option[Seq[DisplayInfo]],
      countryCodes: Option[Seq[String]]
  ): Future[ColumnType[_]] = {
    checkArguments(
      greaterZero(tableId),
      greaterZero(columnId),
      isDefined(
        Seq(columnName, ordering, kind, identifier, displayInfos, countryCodes),
        "name, ordering, kind, identifier, displayInfos, countryCodes"
      )
    )

    val structureProperties: Seq[Option[Any]] = Seq(columnName, ordering, kind, identifier, countryCodes)
    val isAtLeastOneStructureProperty: Boolean = structureProperties.exists(_.isDefined)

    logger.info(
      s"changeColumn $tableId $columnId name=$columnName ordering=$ordering kind=$kind identifier=$identifier " +
        s"displayInfos=$displayInfos, countryCodes=$countryCodes"
    )

    for {
      table <- tableStruc.retrieve(tableId)
      column <- columnStruc.retrieve(table, columnId)

      _ <- if (isAtLeastOneStructureProperty) {
        roleModel.checkAuthorization(EditStructureProperty, ScopeColumn, ComparisonObjects(table, column))
      } else {
        roleModel.checkAuthorization(EditDisplayProperty, ScopeColumn, ComparisonObjects(table, column))
      }

      changedColumn <- table.tableType match {
        case GenericTable =>
          columnStruc.change(table, columnId, columnName, ordering, kind, identifier, displayInfos, countryCodes)
        case SettingsTable => Future.failed(ForbiddenException("can't change a column of a settings table", "column"))
      }

      _ <- CacheClient(this).invalidateColumn(tableId, columnId)
    } yield changedColumn
  }

  def createTableGroup(displayInfos: Seq[DisplayInfo]): Future[TableGroup] = {
    checkArguments(nonEmpty(displayInfos, "displayName or description"))
    logger.info(s"createTableGroup $displayInfos")

    for {
      _ <- roleModel.checkAuthorization(Create, ScopeTableGroup)
      tableGroup <- tableGroupStruc.create(displayInfos)
    } yield tableGroup
  }

  def changeTableGroup(tableGroupId: TableGroupId, displayInfos: Option[Seq[DisplayInfo]]): Future[TableGroup] = {
    checkArguments(greaterZero(tableGroupId), isDefined(displayInfos, "displayName or description"))
    logger.info(s"changeTableGroup $tableGroupId $displayInfos")

    for {
      _ <- roleModel.checkAuthorization(Edit, ScopeTableGroup)
      _ <- tableGroupStruc.change(tableGroupId, displayInfos)
      tableGroup <- tableGroupStruc.retrieve(tableGroupId)
    } yield tableGroup
  }

  def deleteTableGroup(tableGroupId: TableGroupId): Future[EmptyObject] = {
    checkArguments(greaterZero(tableGroupId))
    logger.info(s"deleteTableGroup $tableGroupId")

    for {
      _ <- roleModel.checkAuthorization(Delete, ScopeTableGroup)
      _ <- tableGroupStruc.delete(tableGroupId)
    } yield EmptyObject()
  }
}
