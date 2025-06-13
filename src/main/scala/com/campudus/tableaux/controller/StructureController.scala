package com.campudus.tableaux.controller

import com.campudus.tableaux.{ForbiddenException, InvalidJsonException, TableauxConfig, UnprocessableEntityException}
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{CreateColumn, _}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.structure.{CachedColumnModel, TableGroupModel, TableModel}
import com.campudus.tableaux.database.model.structure.ColumnModel.isColumnGroupMatchingToFormatPattern
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.verticles.EventClient
import com.campudus.tableaux.verticles.ValidatorKeys

import org.vertx.scala.core.json._

import scala.concurrent.Future

object StructureController {

  def apply(config: TableauxConfig, repository: StructureModel, roleModel: RoleModel)(): StructureController = {
    new StructureController(config, repository, roleModel)
  }
}

class StructureController(
    override val config: TableauxConfig,
    override protected val repository: StructureModel,
    implicit protected val roleModel: RoleModel
) extends Controller[StructureModel] {

  val tableStruc: TableModel = repository.tableStruc
  val columnStruc: CachedColumnModel = repository.columnStruc
  val tableGroupStruc: TableGroupModel = repository.tableGroupStruc
  val eventClient: EventClient = EventClient(vertx)

  def retrieveTable(tableId: TableId)(implicit user: TableauxUser): Future[Table] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTable $tableId")

    for {
      table <- tableStruc.retrieve(tableId)
    } yield table
  }

  def retrieveTables()(implicit user: TableauxUser): Future[TableSeq] = {
    logger.info(s"retrieveTables")

    for {
      tableSeq: Seq[Table] <- tableStruc.retrieveAll(isInternalCall = false)
    } yield {
      TableSeq(tableSeq)
    }
  }

  def createColumns(tableId: TableId, columns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[ColumnSeq] = {
    checkArguments(greaterZero(tableId), nonEmpty(columns, "columns"))
    logger.info(s"createColumns $tableId columns $columns")

    for {
      table <- retrieveTable(tableId)
      _ <- roleModel.checkAuthorization(CreateColumn, ComparisonObjects(table))
      created <- table.tableType match {
        case SettingsTable => Future.failed(ForbiddenException("can't add a column to a settings table", "column"))
        case UnionTable => columnStruc.createUnionTableColumns(table, columns)
        case _ => columnStruc.createColumns(table, columns)
      }

      retrieved <- Future.sequence(created.map(c => retrieveColumn(c.table.id, c.id)))
      sorted = retrieved.sortBy(_.ordering)
    } yield {
      sorted.foreach(col => eventClient.columnCreated(tableId, col.id))
      ColumnSeq(sorted)
    }
  }

  def retrieveColumn(tableId: TableId, columnId: ColumnId)(
      implicit user: TableauxUser
  ): Future[ColumnType[_]] = {
    checkArguments(greaterZero(tableId), greaterThan(columnId, -1, "columnId"))
    logger.info(s"retrieveColumn $tableId $columnId")

    for {
      table <- tableStruc.retrieve(tableId)
      column <- columnStruc.retrieve(table, columnId)
      _ <- roleModel.checkAuthorization(ViewColumn, ComparisonObjects(table, column))
    } yield column
  }

  def retrieveColumns(
      tableId: TableId,
      isInternalCall: Boolean = false,
      columnFilter: ColumnFilter = ColumnFilter(None, None)
  )(
      implicit user: TableauxUser
  ): Future[ColumnSeq] = {
    checkArguments(
      greaterZero(tableId),
      columnFilter.check
    )

    logger.info(s"retrieveColumns $tableId, columnFilter: $columnFilter")

    for {
      table <- tableStruc.retrieve(tableId)
      columns <- columnStruc.retrieveAll(table)
    } yield {
      val filteredColumns: Seq[ColumnType[_]] =
        roleModel.filterDomainObjects[ColumnType[_]](
          ViewColumn,
          columns,
          ComparisonObjects(table),
          isInternalCall
        ).filter(columnFilter.filter)
      ColumnSeq(filteredColumns)
    }
  }

  def retrieveStructure()(implicit user: TableauxUser): Future[TablesStructure] = {
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
      tableGroupId: Option[TableGroupId],
      attributes: Option[JsonObject],
      concatFormatPattern: Option[String],
      originTables: Option[Seq[TableId]]
  )(implicit user: TableauxUser): Future[Table] = {
    checkArguments(notNull(tableName, "name"))
    logger.info(
      s"createTable $tableName $hidden $langtags $displayInfos $tableType $tableGroupId $concatFormatPattern $originTables"
    )

    def createTable(anything: Unit): Future[Table] =
      tableType match {
        case SettingsTable => {
          createSettingsTable.apply(
            tableName,
            hidden,
            langtags,
            displayInfos,
            tableGroupId,
            attributes,
            concatFormatPattern
          )
        }
        case TaxonomyTable => createTaxonomyTable.apply(
            tableName,
            hidden,
            langtags,
            displayInfos,
            tableGroupId,
            attributes,
            concatFormatPattern
          )
        case UnionTable =>
          createUnionTable(
            tableName,
            hidden,
            langtags,
            displayInfos,
            tableGroupId,
            attributes,
            concatFormatPattern,
            originTables
          )
        case _ =>
          createGenericTable.apply(
            tableName,
            hidden,
            langtags,
            displayInfos,
            tableGroupId,
            attributes,
            concatFormatPattern
          )
      }

    (attributes match {
      case Some(s) => {
        eventClient.validateJson(ValidatorKeys.ATTRIBUTES, s).flatMap(createTable).recover {
          case ex => throw InvalidJsonException(ex.getMessage(), "attributes")
        }
      }
      case None => createTable(Unit)
    }) map { table =>
      eventClient.tableCreated(table.id)
      table
    }
  }

  private def buildTable(tableType: TableType, columns: Option[Seq[CreateColumn]])(implicit user: TableauxUser) =
    (
        tableName: String,
        hidden: Boolean,
        langtags: Option[Option[Seq[String]]],
        displayInfos: Seq[DisplayInfo],
        tableGroupId: Option[TableGroupId],
        attributes: Option[JsonObject],
        concatFormatPattern: Option[String]
    ) => {
      for {
        _ <- roleModel.checkAuthorization(CreateTable)
        tableStub <- tableStruc.create(
          tableName,
          hidden,
          langtags,
          displayInfos,
          tableType,
          tableGroupId,
          attributes,
          concatFormatPattern,
          None
        )
        _ <- columns match {
          case None => Future.successful(())
          case Some(cols) => columnStruc.createColumns(tableStub, cols)
        }
        table <- tableStruc.retrieve(tableStub.id)
      } yield table
    }

  private def createGenericTable(implicit user: TableauxUser) = buildTable(GenericTable, columns = None)

  private def createSettingsTable(implicit user: TableauxUser) = buildTable(
    SettingsTable,
    Some(Seq(
      CreateSimpleColumn(
        "key",
        None,
        ShortTextType,
        LanguageNeutral,
        identifier = true,
        Seq.empty,
        false,
        Option(Json.obj())
      ),
      CreateSimpleColumn(
        "displayKey",
        None,
        ShortTextType,
        MultiLanguage,
        identifier = false,
        Seq(
          NameOnly("de", "Bezeichnung"),
          NameOnly("en", "Identifier")
        ),
        false,
        Option(Json.obj())
      ),
      CreateSimpleColumn(
        "value",
        None,
        TextType,
        MultiLanguage,
        identifier = false,
        Seq(
          NameOnly("de", "Inhalt"),
          NameOnly("en", "Value")
        ),
        false,
        Option(Json.obj())
      ),
      CreateAttachmentColumn(
        "attachment",
        None,
        identifier = false,
        Seq(
          NameOnly("de", "Anhang"),
          NameOnly("en", "Attachment")
        ),
        None,
        hidden = false
      )
    ))
  )

  private def createTaxonomyTable(implicit user: TableauxUser) = (
      tableName: String,
      hidden: Boolean,
      langtags: Option[Option[Seq[String]]],
      displayInfos: Seq[DisplayInfo],
      tableGroupId: Option[TableGroupId],
      attributes: Option[JsonObject],
      concatFormatPattern: Option[String]
  ) => {
    for {
      tableStub <- buildTable(
        TaxonomyTable,
        Some(
          Seq(
            CreateSimpleColumn(
              "name",
              None,
              ShortTextType,
              MultiLanguage,
              identifier = true,
              Seq(NameOnly("de", "Bezeichnung"), NameOnly("en", "Name")),
              false,
              Option(Json.obj())
            ),
            CreateSimpleColumn(
              "ordering",
              None,
              NumericType,
              LanguageNeutral,
              identifier = false,
              Seq(NameOnly("de", "Reihenfolge"), NameOnly("en", "Ordering")),
              false,
              Option(Json.obj()),
              hidden = true
            ),
            CreateSimpleColumn(
              "code",
              None,
              ShortTextType,
              LanguageNeutral,
              identifier = false,
              Seq(NameOnly("de", "Eindeutige Bezeichnung"), NameOnly("en", "Unique identifier")),
              false,
              Option(Json.obj())
            )
          )
        )
      ).apply(tableName, hidden, langtags, displayInfos, tableGroupId, attributes, concatFormatPattern)
      _ <- columnStruc.createColumn(
        tableStub,
        CreateLinkColumn(
          "parent",
          None,
          tableStub.id,
          Some("parent"),
          None,
          singleDirection = false,
          identifier = false,
          Seq(NameOnly("de", "Oberkategorie"), NameOnly("en", "Parent category")),
          Constraint(Cardinality(0, 1), false, false),
          attributes,
          hidden = true
        )
      )
      table <- tableStruc.retrieve(tableStub.id)
    } yield table
  }

  private def checkOriginTables(originTables: Option[Seq[TableId]])(implicit user: TableauxUser): Future[Unit] = {
    originTables match {
      case Some(tables) if tables.nonEmpty =>
        tables.foreach(tableId => checkArguments(greaterZero(tableId)))

        Future.sequence(tables.map(id => tableStruc.retrieve(id))).map(_ => ())

        Future.successful(())
      case _ => Future.failed(
          UnprocessableEntityException(
            s"originTables must be a non-empty sequence of valid table IDs, was: $originTables"
          )
        )
    }
  }

  private def createUnionTable(
      tableName: String,
      hidden: Boolean,
      langtags: Option[Option[Seq[String]]],
      displayInfos: Seq[DisplayInfo],
      tableGroupId: Option[TableGroupId],
      attributes: Option[JsonObject],
      concatFormatPattern: Option[String],
      originTables: Option[Seq[TableId]]
  )(implicit user: TableauxUser): Future[Table] = {
    for {
      _ <- roleModel.checkAuthorization(CreateTable)
      _ <- checkOriginTables(originTables)
      created <- tableStruc.create(
        tableName,
        hidden,
        langtags,
        displayInfos,
        UnionTable,
        tableGroupId,
        attributes,
        concatFormatPattern,
        originTables
      )
      _ <- columnStruc.createColumn(
        created,
        CreateSimpleColumn(
          "originTable",
          None,
          OriginTableType,
          MultiLanguage,
          identifier = true,
          Seq(
            NameAndDescription("de", "Ursprungstabelle", "Der Tabellenname, aus der die Daten stammen"),
            NameAndDescription("en", "Origin Table", "The name of the table from which the data is taken")
          ),
          false,
          Option(Json.obj())
        )
      )
      retrieved <- tableStruc.retrieve(created.id)
    } yield retrieved
  }

  def deleteTable(tableId: TableId)(implicit user: TableauxUser): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"deleteTable $tableId")

    for {
      table <- tableStruc.retrieve(tableId)
      _ <- roleModel.checkAuthorization(DeleteTable, ComparisonObjects(table))
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
          eventClient.invalidateColumn(tableId, column.id)
        }
      }))
    } yield {
      eventClient.tableDeleted(tableId, table)
      EmptyObject()
    }
  }

  def deleteColumn(tableId: TableId, columnId: ColumnId)(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"deleteColumn $tableId $columnId")

    for {
      table <- tableStruc.retrieve(tableId)
      column <- columnStruc.retrieve(table, columnId)
      _ <- roleModel.checkAuthorization(DeleteColumn, ComparisonObjects(table, column))
      _ <- table.tableType match {
        case GenericTable | UnionTable => columnStruc.delete(table, columnId)
        case TaxonomyTable =>
          if (columnId > 4) columnStruc.delete(table, columnId)
          else Future.failed(ForbiddenException("can't delete a default column from a taxonomy table", "column"))
        case SettingsTable =>
          Future.failed(ForbiddenException("can't delete a column from a settings table", "column"))
      }

      _ <- eventClient.invalidateColumn(tableId, columnId)
    } yield {
      eventClient.columnDeleted(table.id, column.id, column)
      EmptyObject()
    }
  }

  def changeTable(
      tableId: TableId,
      tableName: Option[String],
      hidden: Option[Boolean],
      langtags: Option[Option[Seq[String]]],
      displayInfos: Option[Seq[DisplayInfo]],
      tableGroupId: Option[Option[TableGroupId]],
      attributes: Option[JsonObject],
      concatFormatPattern: Option[String]
  )(implicit user: TableauxUser): Future[Table] = {
    checkArguments(
      greaterZero(tableId),
      isDefined(
        Seq(tableName, hidden, langtags, displayInfos, tableGroupId, attributes, concatFormatPattern),
        "name, hidden, langtags, displayName, description, group, concatFormatPattern"
      )
    )

    val structureProperties: Seq[Option[Any]] = Seq(tableName, hidden, langtags, tableGroupId)
    val isAtLeastOneStructureProperty: Boolean = structureProperties.exists(_.isDefined)

    logger.info(s"changeTable $tableId $tableName $hidden $langtags $displayInfos $tableGroupId $concatFormatPattern")

    for {
      table <- tableStruc.retrieve(tableId)
      _ <-
        if (isAtLeastOneStructureProperty) {
          roleModel.checkAuthorization(EditTableStructureProperty, ComparisonObjects(table))
        } else {
          roleModel.checkAuthorization(EditTableDisplayProperty, ComparisonObjects(table))
        }
      _ <-
        if (attributes.nonEmpty) {
          eventClient
            .validateJson(ValidatorKeys.ATTRIBUTES, attributes.get)
            .recover({
              case ex => throw new InvalidJsonException(ex.getMessage(), "attributes")
            })
        } else {
          Future { Unit }
        }
      _ <- tableStruc.change(
        tableId,
        tableName,
        hidden,
        langtags,
        displayInfos,
        tableGroupId,
        attributes,
        concatFormatPattern
      )
      changedTable <- tableStruc.retrieve(tableId)
    } yield {
      logger.info(s"retrieved table after change $changedTable")
      eventClient.tableChanged(tableId)
      columnStruc.removeCache(table.id, None)
      changedTable
    }
  }

  def changeTableOrder(tableId: TableId, locationType: LocationType)(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"changeTableOrder $tableId $locationType")

    for {
      table <- tableStruc.retrieve(tableId)
      _ <- roleModel.checkAuthorization(EditTableDisplayProperty, ComparisonObjects(table))
      _ <- tableStruc.changeOrder(tableId, locationType)
    } yield EmptyObject()
  }

  def changeColumn(
      tableId: TableId,
      columnId: ColumnId,
      columnName: Option[String],
      ordering: Option[Ordering] = None,
      kind: Option[TableauxDbType] = None,
      identifier: Option[Boolean] = None,
      displayInfos: Option[Seq[DisplayInfo]] = None,
      countryCodes: Option[Seq[String]] = None,
      separator: Option[Boolean] = None,
      attributes: Option[JsonObject] = None,
      rules: Option[JsonArray] = None,
      hidden: Option[Boolean] = None,
      maxLength: Option[Int] = None,
      minLength: Option[Int] = None,
      showMemberColumns: Option[Boolean] = None,
      decimalDigits: Option[Int] = None,
      formatPattern: Option[String] = None
  )(implicit user: TableauxUser): Future[ColumnType[_]] = {
    checkArguments(
      greaterZero(tableId),
      greaterZero(columnId),
      isDefined(
        Seq(
          columnName,
          ordering,
          kind,
          identifier,
          displayInfos,
          countryCodes,
          separator,
          attributes,
          rules,
          hidden,
          maxLength,
          minLength,
          showMemberColumns,
          decimalDigits,
          formatPattern
        ),
        "name, ordering, kind, identifier, displayInfos, countryCodes, separator, attributes, " +
          "rules, hidden, maxLength, minLength, showMemberColumns, decimalDigits, formatPattern"
      )
    )

    val structureProperties: Seq[Option[Any]] = Seq(columnName, ordering, kind, identifier, countryCodes)
    val isAtLeastOneStructureProperty: Boolean = structureProperties.exists(_.isDefined)

    logger.info(
      s"changeColumn $tableId $columnId name=$columnName ordering=$ordering kind=$kind identifier=$identifier separator=$separator" +
        s"displayInfos=$displayInfos, countryCodes=$countryCodes"
    )

    val performChangeFx = (table: Table) =>
      columnStruc
        .change(
          table,
          columnId,
          columnName,
          ordering,
          kind,
          identifier,
          displayInfos,
          countryCodes,
          separator,
          attributes,
          rules,
          hidden,
          maxLength,
          minLength,
          showMemberColumns,
          decimalDigits,
          formatPattern
        )

    for {
      table <- tableStruc.retrieve(tableId)
      column <- columnStruc.retrieve(table, columnId)
      _ <-
        if (attributes.nonEmpty) {
          eventClient
            .validateJson(ValidatorKeys.ATTRIBUTES, attributes.get)
            .recover({
              case ex => throw new InvalidJsonException(ex.getMessage(), "attributes")
            })
        } else {
          Future { Unit }
        }

      _ <-
        if (rules.nonEmpty) {
          for {
            _ <- eventClient
              .validateJson(ValidatorKeys.STATUS, rules.get)
              .recover({
                case ex => throw new InvalidJsonException(ex.getMessage(), "rules")
              })
            _ <- columnStruc.retrieveAndValidateDependentStatusColumns(rules.get, table)
          } yield ()
        } else {
          Future { Unit }
        }

      _ <-
        if (formatPattern.isDefined) {
          column match {
            case groupColumn: GroupColumn => {
              if (!isColumnGroupMatchingToFormatPattern(formatPattern, groupColumn.columns)) {
                val columnsIds = groupColumn.columns.map(_.id).mkString(", ");

                Future.failed(UnprocessableEntityException(
                  s"Invalid formatPattern: columns ($columnsIds) don't match with formatPattern '$formatPattern'"
                ))
              } else {
                Future.successful(())
              }
            }
            case _ =>
              Future.failed(ForbiddenException(
                s"Update of formatPattern '$formatPattern' is not allowed for column ${column.kind}.",
                "column"
              ))
          }
        } else {
          Future.successful(())
        }

      _ <-
        if (isAtLeastOneStructureProperty) {
          roleModel.checkAuthorization(EditColumnStructureProperty, ComparisonObjects(table, column))
        } else {
          roleModel.checkAuthorization(EditColumnDisplayProperty, ComparisonObjects(table, column))
        }

      changedColumn <- table.tableType match {
        case GenericTable => performChangeFx(table)
        case SettingsTable => Future.failed(ForbiddenException("can't change a column of a settings table", "column"))
        case UnionTable => Future.failed(ForbiddenException("TODO not implemented yet", "column"))
        case TaxonomyTable =>
          if (columnId > 4) performChangeFx(table)
          else Future.failed(ForbiddenException("can't change a default column of a taxonomy table", "column"))
      }

      _ <- eventClient.invalidateColumn(tableId, columnId)
    } yield changedColumn
  }

  def createTableGroup(displayInfos: Seq[DisplayInfo])(implicit user: TableauxUser): Future[TableGroup] = {
    checkArguments(nonEmpty(displayInfos, "displayName or description"))
    logger.info(s"createTableGroup $displayInfos")

    for {
      _ <- roleModel.checkAuthorization(CreateTableGroup)
      tableGroup <- tableGroupStruc.create(displayInfos)
    } yield tableGroup
  }

  def changeTableGroup(tableGroupId: TableGroupId, displayInfos: Option[Seq[DisplayInfo]])(
      implicit user: TableauxUser
  ): Future[TableGroup] = {
    checkArguments(greaterZero(tableGroupId), isDefined(displayInfos, "displayName or description"))
    logger.info(s"changeTableGroup $tableGroupId $displayInfos")

    for {
      _ <- roleModel.checkAuthorization(EditTableGroup)
      _ <- tableGroupStruc.change(tableGroupId, displayInfos)
      tableGroup <- tableGroupStruc.retrieve(tableGroupId)
    } yield tableGroup
  }

  def deleteTableGroup(tableGroupId: TableGroupId)(implicit user: TableauxUser): Future[EmptyObject] = {
    checkArguments(greaterZero(tableGroupId))
    logger.info(s"deleteTableGroup $tableGroupId")

    for {
      _ <- roleModel.checkAuthorization(DeleteTableGroup)
      _ <- tableGroupStruc.delete(tableGroupId)
    } yield EmptyObject()
  }
}
