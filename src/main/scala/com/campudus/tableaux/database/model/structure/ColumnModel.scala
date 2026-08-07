package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux._
import com.campudus.tableaux.{
  HasStatusColumnDependencyException,
  WrongLanguageTypeException,
  WrongStatusColumnKindException,
  WrongStatusConditionTypeException
}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.structure.CachedColumnModel._
import com.campudus.tableaux.database.model.structure.ColumnModel.isColumnGroupMatchingToFormatPattern
import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.helper.JsonUtils.asSeqOf
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.router.auth.permission.TableauxUser
import com.campudus.tableaux.verticles.EventClient
import com.campudus.tableaux.verticles.ValidatorKeys

import io.vertx.core.Vertx
import io.vertx.ext.web.RoutingContext
import io.vertx.lang.scala.json._

import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import com.google.common.cache.{Cache => GuavaCache, CacheBuilder}
import com.typesafe.scalalogging.LazyLogging
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import org.checkerframework.checker.units.qual

object CachedColumnModel {

  /**
    * Default never expire
    */
  val DEFAULT_EXPIRE_AFTER_ACCESS: Long = -1L

  /**
    * Max. 100k cached values per column
    */
  val DEFAULT_MAXIMUM_SIZE: Long = 100000L
}

class CachedColumnModel(
    val config: JsonObject,
    override val connection: DatabaseConnection
)(
    implicit roleModel: RoleModel
) extends ColumnModel(connection) {

  private val cache: GuavaCache[String, Object] = createCache()

  private def cacheKey(parts: Seq[Any]): String = parts.mkString(":")

  private def cachingF[A](parts: Any*)(f: => Future[A]): Future[A] = {
    val key = cacheKey(parts)

    Option(cache.getIfPresent(key)) match {
      case Some(value) => Future.successful(value.asInstanceOf[A])
      case None =>
        f.andThen({
          case Success(value) => cache.put(key, value.asInstanceOf[Object])
        })
    }
  }

  private def remove(parts: Any*): Future[Unit] = {
    cache.invalidate(cacheKey(parts))
    Future.successful(())
  }

  private def removeAll(): Future[Unit] = {
    cache.invalidateAll()
    Future.successful(())
  }

  private def createCache() = {
    val builder = CacheBuilder.newBuilder()
    logger.info(
      s"CachedColumnModel initialized: DEFAULT_MAXIMUM_SIZE: $DEFAULT_MAXIMUM_SIZE"
        + s", DEFAULT_EXPIRE_AFTER_ACCESS: $DEFAULT_EXPIRE_AFTER_ACCESS"
    )

    val expireAfterAccess = config.getLong("expireAfterAccess", DEFAULT_EXPIRE_AFTER_ACCESS).longValue()
    if (expireAfterAccess > 0) {
      builder.expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
    }

    val maximumSize = config.getLong("maximumSize", DEFAULT_MAXIMUM_SIZE).longValue()
    if (maximumSize > 0) {
      builder.maximumSize(maximumSize)
    }

    builder.recordStats()

    builder.build[String, Object]()
  }

  def removeAllCache(): Future[Unit] = {
    for {
      _ <- removeAll()
    } yield ()
  }

  def removeCache(tableId: TableId, columnIdOpt: Option[ColumnId]): Future[Unit] = {

    for {
      // remove retrieveAll cache
      _ <- remove("retrieveAll", tableId)

      // remove retrieve cache (of column itself)
      _ <- columnIdOpt match {
        case Some(columnId) => remove("retrieve", tableId, columnId)
        case None => Future.successful(())
      }

      // remove retrieve cache of depending group columns
      _ <- columnIdOpt match {
        case Some(columnId) =>
          for {
            dependentGroupColumns <- retrieveDependentGroupColumn(tableId, columnId)
            _ <- Future.sequence(dependentGroupColumns.map({
              case DependentColumnInformation(dependentTableId, groupColumnId, _, _, _) =>
                remove("retrieve", tableId, groupColumnId)
                remove("retrieveAll", tableId)
            }))
          } yield ()
        case None =>
          Future.successful(())
      }

      // remove retrieve & retrieveAll cache of depending link columns
      dependencies <- retrieveDependencies(tableId)
      _ <- Future.sequence(dependencies.map({
        case DependentColumnInformation(dependentTableId, dependentColumnId, _, _, groupColumnIds) =>
          for {
            _ <- remove("retrieve", dependentTableId, dependentColumnId)
            _ <- Future.sequence(groupColumnIds.map(remove("retrieve", dependentTableId, _)))
            _ <- remove("retrieveAll", dependentTableId)
          } yield ()
      }))
    } yield ()
  }

  override def retrieve(table: Table, columnId: ColumnId)(
      implicit user: TableauxUser
  ): Future[ColumnType[?]] = {
    cachingF("retrieve", table.id, columnId)(
      super.retrieve(table, columnId)
    )
  }

  override def retrieveAll(table: Table)(implicit user: TableauxUser): Future[Seq[ColumnType[?]]] = {
    cachingF("retrieveAll", table.id)(
      super.retrieveAll(table)
    )
  }

  override def createColumns(table: Table, createColumns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {
    for {
      r <- super.createColumns(table, createColumns)
      _ <- removeCache(table.id, None)
    } yield r
  }

  override def createUnionTableColumns(table: Table, createColumns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {
    for {
      r <- super.createUnionTableColumns(table, createColumns)
      _ <- removeCache(table.id, None)
    } yield r
  }

  override def createColumn(table: Table, createColumn: CreateColumn)(
      implicit user: TableauxUser
  ): Future[ColumnType[?]] = {
    for {
      r <- super.createColumn(table, createColumn)
      _ <- removeCache(table.id, None)
    } yield r
  }

  override def delete(
      table: Table,
      columnId: ColumnId,
      bothDirections: Boolean,
      checkForLastColumn: Boolean = true
  )(implicit user: TableauxUser): Future[Unit] = {
    for {
      _ <- removeCache(table.id, Some(columnId))
      r <- super.delete(table, columnId, bothDirections, checkForLastColumn)
      _ <- removeCache(table.id, Some(columnId))
    } yield r
  }

  override def change(
      table: Table,
      columnId: ColumnId,
      columnName: Option[String],
      ordering: Option[Ordering],
      kind: Option[TableauxDbType],
      identifier: Option[Boolean],
      displayInfos: Option[Seq[DisplayInfo]],
      countryCodes: Option[Seq[String]],
      separator: Option[Boolean],
      attributes: Option[JsonObject],
      rules: Option[JsonArray],
      hidden: Option[Boolean],
      maxLength: Option[Int],
      minLength: Option[Int],
      showMemberColumns: Option[Boolean],
      decimalDigits: Option[Int],
      formatPattern: Option[String],
      linkAttributes: Option[Seq[LinkAttributeDefinition]]
  )(implicit user: TableauxUser): Future[ColumnType[?]] = {
    for {
      _ <- removeCache(table.id, Some(columnId))
      r <- super
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
          formatPattern,
          linkAttributes
        )
    } yield r
  }

  override def retrieveAndValidateDependentStatusColumns(rules: JsonArray, table: Table)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {
    super.retrieveAndValidateDependentStatusColumns(rules, table)
  }
}

object ColumnModel extends LazyLogging {

  def isColumnGroupMatchingToFormatPattern(
      formatPattern: Option[String],
      groupedColumns: Seq[ColumnType[?]]
  ): Boolean = {
    val formatVariable = "\\{\\{(\\w+)\\}\\}".r

    formatPattern match {
      case Some(patternString) => {
        val distinctWildcards =
          formatVariable
            .findAllMatchIn(patternString)
            .toSeq
            .flatMap(_.subgroups)
            .distinct
            .to(SortedSet)

        val columnIDs = groupedColumns.map(_.id).map(_.toString).to(SortedSet)

        logger.info(
          s"Compare distinct wildcards (${distinctWildcards.mkString(", ")}) " +
            s"with columnIDs (${columnIDs.mkString(", ")})"
        )

        distinctWildcards.subsetOf(columnIDs)
      }
      case None => true
    }
  }

  // Kept as its own regex/val (rather than reusing isColumnGroupMatchingToFormatPattern's) so GroupColumn's existing
  // numeric-column-id-only wildcard behaviour is unaffected by allowing dotted paths here (e.g. attributes.percentage).
  def isLinkColumnMatchingToFormatPattern(
      formatPattern: Option[String],
      linkAttributes: Seq[LinkAttributeDefinition]
  ): Boolean = {
    val formatVariable = "\\{\\{([\\w.]+)\\}\\}".r

    formatPattern match {
      case Some(patternString) => {
        val distinctWildcards =
          formatVariable
            .findAllMatchIn(patternString)
            .toSeq
            .flatMap(_.subgroups)
            .distinct
            .to(SortedSet)

        val allowedTokens = (Set("value") ++ linkAttributes.map(a => s"attributes.${a.name}")).to(SortedSet)

        logger.info(
          s"Compare distinct wildcards (${distinctWildcards.mkString(", ")}) " +
            s"with allowed link tokens (${allowedTokens.mkString(", ")})"
        )

        distinctWildcards.subsetOf(allowedTokens)
      }
      case None => true
    }
  }
}

class ColumnModel(val connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {

  private lazy val tableStruc = new TableModel(connection)

  private val MAX_DEPTH = 5

  def createColumns(table: Table, createColumns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {
    createColumns.foldLeft(Future.successful(Seq.empty[ColumnType[?]])) {
      case (future, next) =>
        for {
          createdColumns <- future
          createdColumn <- createColumn(table, next)
        } yield {
          createdColumns :+ createdColumn
        }
    }
  }

  def createColumn(table: Table, createColumn: CreateColumn)(
      implicit user: TableauxUser
  ): Future[ColumnType[?]] = {

    val attributes = createColumn.attributes
    val validator = EventClient(Vertx.currentContext().owner())

    def applyColumnInformation(id: ColumnId, ordering: Ordering, displayInfos: Seq[DisplayInfo]) =
      BasicColumnInformation(table, id, ordering, displayInfos, createColumn)

    for {
      _ <-
        if (attributes.nonEmpty) {
          validator
            .validateJson(ValidatorKeys.ATTRIBUTES, attributes.get)
            .recover({
              case ex => throw new InvalidJsonException(ex.getMessage(), "attributes")
            })
        } else {
          Future(())
        }
      columnCreated <- createColumn match {
        case simpleColumnInfo: CreateSimpleColumn =>
          createValueColumn(table, simpleColumnInfo)
            .map({
              case CreatedColumnInformation(_, id, ordering, displayInfos) =>
                SimpleValueColumn(
                  simpleColumnInfo.kind,
                  simpleColumnInfo.languageType,
                  applyColumnInformation(id, ordering, displayInfos)
                )
            })

        case linkColumnInfo: CreateLinkColumn =>
          createLinkColumn(table, linkColumnInfo)
            .map({
              case (linkId, toCol, CreatedColumnInformation(_, id, ordering, displayInfos)) =>
                val linkDirection = LeftToRight(table.id, linkColumnInfo.toTable, linkColumnInfo.constraint)
                LinkColumn(
                  applyColumnInformation(id, ordering, displayInfos),
                  toCol,
                  linkId,
                  linkDirection,
                  linkColumnInfo.linkAttributes,
                  linkColumnInfo.formatPattern
                )
            })

        case attachmentColumnInfo: CreateAttachmentColumn =>
          createAttachmentColumn(table.id, attachmentColumnInfo)
            .map({
              case CreatedColumnInformation(_, id, ordering, displayInfos) =>
                AttachmentColumn(applyColumnInformation(id, ordering, displayInfos))
            })

        case groupColumnInfo: CreateGroupColumn => {
          createGroupColumn(table, groupColumnInfo)
            .map({
              case CreatedColumnInformation(_, id, ordering, displayInfos) =>
                // For simplification we return GroupColumn without grouped columns...
                // ... StructureController will retrieve these anyway
                GroupColumn(
                  applyColumnInformation(id, ordering, displayInfos),
                  Seq.empty,
                  groupColumnInfo.formatPattern,
                  groupColumnInfo.showMemberColumns
                )
            })
        }

        case statusColumnInfo: CreateStatusColumn =>
          for {
            _ <- validator.validateJson(ValidatorKeys.STATUS, statusColumnInfo.rules).recover {
              case ex => throw new InvalidJsonException(ex.getMessage(), "rules")
            }

            statusColumn <- createStatusColumn(table, statusColumnInfo).map({
              case (dependentColumns, CreatedColumnInformation(_, id, ordering, displayInfos)) =>
                StatusColumn(
                  StatusColumnInformation(table, id, ordering, displayInfos, statusColumnInfo),
                  statusColumnInfo.rules,
                  dependentColumns
                )

            })
          } yield {
            statusColumn
          }
      }
    } yield {
      columnCreated
    }
  }

  private def checkCreateUnionColumns(unionTable: Table, createColumns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[String]] = {

    val initialErrors = createColumns.foldLeft(Seq.empty[String]) {
      case (errors, createColumn) =>
        createColumn.originColumns match {
          case Some(_) => errors
          case None =>
            errors :+ s"CreateColumn '${createColumn.name}' has no valid field originColumns"
        }
    }

    val table2CreateColumn = createColumns.foldLeft(Map.empty[TableId, Map[CreateColumn, Set[ColumnId]]]) {
      case (acc, createColumn) =>
        createColumn.originColumns match {
          case Some(CreateOriginColumns(tableToColumnMap)) =>
            tableToColumnMap.foldLeft(acc) {
              case (innerAcc, (tableId, columnId)) =>
                val updatedCreateColumns = innerAcc.get(tableId) match {
                  case Some(colMap) =>
                    val updatedSet = colMap.get(createColumn) match {
                      case Some(colSet) => colSet + columnId
                      case None => Set(columnId)
                    }
                    colMap + (createColumn -> updatedSet)
                  case None =>
                    Map(createColumn -> Set(columnId))
                }
                innerAcc + (tableId -> updatedCreateColumns)
            }
          case None => acc
        }
    }

    val unionTableOriginTables = unionTable.originTables.getOrElse(Seq.empty).toSet
    val missingOriginTableIds = table2CreateColumn.keys.filterNot(unionTableOriginTables.contains).toList
    val originTableError =
      if (missingOriginTableIds.nonEmpty) {
        Seq(
          s"At least one CreateColumn contains originColumns for tables which are not defined in originTables "
            + s"of the union table. Invalid tableIds: (${missingOriginTableIds.mkString(", ")})"
        )
      } else {
        Seq.empty
      }

    for {
      validationErrors <- Future.sequence(table2CreateColumn.toSeq.map({
        case (originTableId, createColumn2OriginColumnIds) =>
          (for {
            originTable <- tableStruc.retrieve(originTableId)
            originColumns <- retrieveAll(originTable)
          } yield {
            createColumn2OriginColumnIds.flatMap {
              case (createColumn, columnIds) =>
                columnIds.flatMap { columnId =>
                  val matchingOriginColumnOpt = originColumns.find(_.id == columnId)
                  matchingOriginColumnOpt match {
                    case Some(originColumn) =>
                      val errorPrefix = s"Column '${originColumn.id}' in table '$originTableId' and " +
                        s"CreateColumn '${createColumn.name}' have different values in field"
                      val errors = Seq.newBuilder[String]
                      if (originColumn.kind != createColumn.kind) {
                        errors += s"$errorPrefix kind: ${originColumn.kind} != ${createColumn.kind}"
                      }
                      if (originColumn.languageType != createColumn.languageType) {
                        errors += s"$errorPrefix languageType: ${originColumn.languageType} != ${createColumn.languageType}"
                      }
                      errors.result()
                    case None =>
                      Seq(s"Column '${columnId}' not found in table '$originTableId'")
                  }
                }
            }
          }).recover({
            case ex => Seq(s"Table '$originTableId' could not be checked, possibly it does not exist")
          })
      }))
    } yield {
      initialErrors ++ originTableError ++ validationErrors.flatten
    }
  }

  def createUnionTableColumns(table: Table, createColumns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {
    for {
      errors <- checkCreateUnionColumns(table, createColumns)
      _ <- {
        if (errors.nonEmpty) {
          Future.failed(InvalidJsonException(errors.mkString(", "), "unionTable"))
        } else {
          Future.successful(())
        }
      }

      result <- createColumns.foldLeft(Future.successful(Seq.empty[ColumnType[?]])) {
        case (future, next) =>
          for {
            createdColumns <- future
            createdColumn <- createUnionTableColumn(table, next)
          } yield {
            createdColumns :+ createdColumn
          }
      }
    } yield result
  }

  def createUnionTableColumn(table: Table, createColumn: CreateColumn)(
      implicit user: TableauxUser
  ): Future[ColumnType[?]] = {
    val attributes = createColumn.attributes
    val validator = EventClient(Vertx.currentContext().owner())

    def applyColumnInformation(id: ColumnId, ordering: Ordering, displayInfos: Seq[DisplayInfo]) =
      BasicColumnInformation(table, id, ordering, displayInfos, createColumn)

    for {

      columnCreate <- createUnionColumn(table, createColumn)
        .map({
          case CreatedColumnInformation(_, id, ordering, displayInfos) =>
            val tableId2ColumnId = createColumn.originColumns match {
              case Some(CreateOriginColumns(tableToColumnMap)) => tableToColumnMap
              case None => Map.empty[TableId, ColumnId]
            }
            UnionColumn(
              createColumn.kind,
              createColumn.languageType,
              applyColumnInformation(id, ordering, displayInfos),
              OriginColumns(tableId2ColumnId)
            )
        })
    } yield {
      columnCreate
    }
  }

  private def createStatusColumn(
      table: Table,
      statusColumnInfo: CreateStatusColumn
  )(implicit user: TableauxUser): Future[(Seq[ColumnType[?]], CreatedColumnInformation)] = {
    connection.transactional { t =>
      for {
        dependentColumns <- retrieveAndValidateDependentStatusColumns(statusColumnInfo.rules, table)
        (t, columnInfo) <- insertSystemColumn(t, table.id, statusColumnInfo, None, None, false)
      } yield {
        (t, (dependentColumns, columnInfo))
      }
    }
  }

  private def createGroupColumn(
      table: Table,
      cgc: CreateGroupColumn
  )(implicit user: TableauxUser): Future[CreatedColumnInformation] = {
    val tableId = table.id

    def resolveGroupNames(columns: Seq[ColumnType[?]], groupNames: Seq[String]): Seq[ColumnId] = {
      if (groupNames.isEmpty) {
        Seq.empty
      } else {
        val columnsByName = columns.groupBy(_.name)
        val (resolvedIds, missingNames, ambiguousNames) = groupNames.foldLeft(
          (Vector.empty[ColumnId], Vector.empty[String], Vector.empty[String])
        ) {
          case ((ids, missing, ambiguous), name) =>
            columnsByName.get(name) match {
              case None => (ids, missing :+ name, ambiguous)
              case Some(matching) if matching.size > 1 => (ids, missing, ambiguous :+ name)
              case Some(matching) => (ids :+ matching.head.id, missing, ambiguous)
            }
        }

        if (missingNames.nonEmpty || ambiguousNames.nonEmpty) {
          val missingPart =
            if (missingNames.nonEmpty) Some(s"Unknown column names: ${missingNames.mkString(", ")}") else None
          val ambiguousPart =
            if (ambiguousNames.nonEmpty) Some(s"Ambiguous column names: ${ambiguousNames.mkString(", ")}") else None
          val details = Seq(missingPart, ambiguousPart).flatten.mkString(". ")

          throw UnprocessableEntityException(
            s"GroupColumn (${cgc.name}) couldn't be created. $details"
          )
        }

        resolvedIds
      }
    }

    def transformFormatPattern(
        formatPattern: Option[String],
        groupNames: Seq[String],
        columns: Seq[ColumnType[?]]
    ): Option[String] = {
      if (groupNames.isEmpty || formatPattern.isEmpty) {
        formatPattern
      } else {
        val columnsByName = columns.groupBy(_.name)
        formatPattern.map(pattern =>
          groupNames.foldLeft(pattern) {
            case (currentPattern, name) =>
              columnsByName.get(name) match {
                case Some(matchingColumns) if matchingColumns.size == 1 =>
                  val columnId = matchingColumns.head.id
                  currentPattern.replaceAll(s"\\{\\{$name\\}\\}", s"{{$columnId}}")
                case _ =>
                  // Name not found or ambiguous - should have been caught by resolveGroupNames
                  currentPattern
              }
          }
        )
      }
    }

    connection.transactional { t =>
      for {
        // retrieve all to-be-grouped columns
        allColumns <- retrieveAll(table)
        groupIds =
          if (cgc.groups.nonEmpty) {
            cgc.groups
          } else {
            resolveGroupNames(allColumns, cgc.groupNames)
          }
        groupedColumns = allColumns.filter(column => groupIds.contains(column.id))
        transformedFormatPattern = transformFormatPattern(cgc.formatPattern, cgc.groupNames, allColumns)

        // do some validation before creating GroupColumn
        _ = {
          if (groupedColumns.size != groupIds.size) {
            throw UnprocessableEntityException(
              s"GroupColumn (${cgc.name}) couldn't be created because some columns don't exist"
            )
          }

          if (groupedColumns.exists(_.kind == GroupType)) {
            throw UnprocessableEntityException(
              s"GroupColumn (${cgc.name}) can't contain another GroupColumn"
            )
          }

          if (!isColumnGroupMatchingToFormatPattern(transformedFormatPattern, groupedColumns)) {
            throw UnprocessableEntityException(
              s"GroupColumns (${groupedColumns.map(_.id).mkString(", ")}) don't match to formatPattern " +
                s"${"\"" + transformedFormatPattern.map(_.toString).orNull + "\""}"
            )
          }
        }

        (t, columnInfo) <- insertSystemColumn(t, tableId, cgc, None, transformedFormatPattern, cgc.showMemberColumns)

        // insert group information
        insertPlaceholder = groupIds.map(_ => "(?, ?, ?)").mkString(", ")
        (t, _) <- t.query(
          s"INSERT INTO system_column_groups(table_id, group_column_id, grouped_column_id) VALUES $insertPlaceholder",
          Json.arr(groupIds.flatMap(Seq(tableId, columnInfo.columnId, _))*)
        )
      } yield (t, columnInfo)
    }
  }

  private def insertSystemUnionColumn(
      t: DbTransaction,
      table: Table,
      createColumn: CreateColumn,
      columnId: ColumnId
  ): Future[(DbTransaction, JsonObject)] = {
    val tableId = table.id
    val originColumns = createColumn.originColumns

    originColumns match {
      case Some(CreateOriginColumns(tableToColumnMap)) =>
        // Create INSERT statement for each origin column mapping
        val insertPlaceholder = tableToColumnMap.map(_ => "(?, ?, ?, ?)").mkString(", ")
        val values = tableToColumnMap.flatMap { case (originTableId, originColumnId) =>
          Seq(tableId, columnId, originTableId, originColumnId)
        }.toSeq

        for {
          (t, _) <- t.query(
            s"INSERT INTO system_union_column(table_id, column_id, origin_table_id, origin_column_id) VALUES $insertPlaceholder",
            Json.arr(values*)
          )
        } yield (t, Json.obj())

      case None =>
        // No origin columns specified, return empty result
        Future.successful((t, Json.obj()))
    }
  }

  private def insertColumnInUserTable(
      t: DbTransaction,
      table: Table,
      simpleColumnInfo: CreateSimpleColumn,
      columnId: ColumnId
  ): Future[(DbTransaction, JsonObject)] = {
    val tableId = table.id

    val tableSql = simpleColumnInfo.languageType match {
      case MultiLanguage | _: MultiCountry => s"user_table_lang_$tableId"
      case LanguageNeutral => s"user_table_$tableId"
    }

    simpleColumnInfo.kind match {
      case BooleanType => t.query(s"ALTER TABLE $tableSql ADD column_${columnId} BOOLEAN DEFAULT false")
      case _ => t.query(s"ALTER TABLE $tableSql ADD column_${columnId} ${simpleColumnInfo.kind.toDbType}")
    }
  }

  private def createValueColumn(
      table: Table,
      simpleColumnInfo: CreateSimpleColumn
  ): Future[CreatedColumnInformation] = {
    val tableId = table.id
    connection.transactional { t =>
      for {
        (t, columnInfo) <- insertSystemColumn(t, tableId, simpleColumnInfo, None, None, false)

        (t, _) <- table.tableType match {
          case UnionTable => insertSystemUnionColumn(t, table, simpleColumnInfo, columnInfo.columnId)
          case _ => insertColumnInUserTable(t, table, simpleColumnInfo, columnInfo.columnId)
        }
      } yield {
        (t, columnInfo)
      }
    }
  }

  private def createUnionColumn(
      table: Table,
      createColumn: CreateColumn
  ): Future[CreatedColumnInformation] = {
    val tableId = table.id
    connection.transactional { t =>
      for {
        (t, columnInfo) <- insertSystemColumn(t, tableId, createColumn, None, None, false)
        (t, _) <- insertSystemUnionColumn(t, table, createColumn, columnInfo.columnId)
      } yield {
        (t, columnInfo)
      }
    }
  }

  private def createAttachmentColumn(
      tableId: TableId,
      attachmentColumnInfo: CreateAttachmentColumn
  ): Future[CreatedColumnInformation] = {
    connection.transactional { t =>
      for {
        (t, columnInfo) <- insertSystemColumn(t, tableId, attachmentColumnInfo, None, None, false)
      } yield (t, columnInfo)
    }
  }

  private def createLinkColumn(
      table: Table,
      linkColumnInfo: CreateLinkColumn
  )(implicit user: TableauxUser): Future[(LinkId, ColumnType[?], CreatedColumnInformation)] = {
    val tableId = table.id

    connection.transactional { t =>
      for {
        toTable <- tableStruc.retrieve(linkColumnInfo.toTable)
        toTableColumns <- retrieveAll(toTable).flatMap({ columns =>
          if (columns.isEmpty) {
            Future.failed(
              NotFoundInDatabaseException(s"Link points at table ${toTable.id} without columns", "no-columns")
            )
          } else {
            Future.successful(columns)
          }
        })

        toCol = toTableColumns.head

        (t, result) <- t.query(
          """|INSERT INTO system_link_table (
             |  table_id_1,
             |  table_id_2,
             |  cardinality_1,
             |  cardinality_2,
             |  delete_cascade,
             |  archive_cascade,
             |  final_cascade,
             |  attributes
             |) VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb) RETURNING link_id""".stripMargin,
          Json.arr(
            tableId,
            linkColumnInfo.toTable,
            linkColumnInfo.constraint.cardinality.from,
            linkColumnInfo.constraint.cardinality.to,
            linkColumnInfo.constraint.deleteCascade,
            linkColumnInfo.constraint.archiveCascade,
            linkColumnInfo.constraint.finalCascade,
            Json.arr(linkColumnInfo.linkAttributes.map(LinkAttributeDefinition.getJson)*).encode()
          )
        )
        linkId = insertNotNull(result).head.get[Long](0)

        // insert link column on source table
        (t, columnInfo) <- insertSystemColumn(t, tableId, linkColumnInfo, Some(linkId), linkColumnInfo.formatPattern, false)

        // only add the second link column if tableId != toTableId or singleDirection is false
        t <- {
          if (!linkColumnInfo.singleDirection && tableId != linkColumnInfo.toTable) {
            val copiedLinkColumnInfo = linkColumnInfo.copy(
              name = linkColumnInfo.foreignLinkColumn.name.getOrElse(table.name),
              identifier = false,
              displayInfos = linkColumnInfo.foreignLinkColumn.displayInfos.getOrElse({
                table.displayInfos.collect({
                  case DisplayInfo(langtag, Some(name), _) =>
                    NameOnly(langtag, name)
                })
              }),
              ordering = linkColumnInfo.foreignLinkColumn.ordering,
              foreignLinkColumn = CreateBackLinkColumn(None, None, None)
            )

            // ColumnInfo will be ignored, so we can lose it
            insertSystemColumn(t, linkColumnInfo.toTable, copiedLinkColumnInfo, Some(linkId), None, false)
              .map({
                case (t, _) => t
              })
          } else {
            Future(t)
          }
        }

        (t, _) <- t.query(s"""|CREATE TABLE link_table_$linkId (
                              |  id_1 bigint,
                              |  id_2 bigint,
                              |  ordering_1 serial,
                              |  ordering_2 serial,
                              |  attributes jsonb,
                              |
                              |  PRIMARY KEY(id_1, id_2),
                              |  
                              |  CONSTRAINT link_table_${linkId}_foreign_1
                              |  FOREIGN KEY(id_1) REFERENCES user_table_$tableId (id) ON DELETE CASCADE,
                              |  CONSTRAINT link_table_${linkId}_foreign_2
                              |  FOREIGN KEY(id_2) REFERENCES user_table_${linkColumnInfo.toTable} (id) ON DELETE CASCADE
                              |)""".stripMargin)
      } yield {
        (t, (linkId, toCol, columnInfo))
      }
    }
  }

  private def insertSystemColumn(
      t: DbTransaction,
      tableId: TableId,
      createColumn: CreateColumn,
      linkId: Option[LinkId],
      formatPattern: Option[String],
      showMemberColumns: Boolean
  ): Future[(DbTransaction, CreatedColumnInformation)] = {

    def insertStatement(tableId: TableId, ordering: String) = {
      s"""|INSERT INTO system_columns (
          |  table_id,
          |  column_id,
          |  column_type,
          |  user_column_name,
          |  ordering,
          |  link_id,
          |  multilanguage,
          |  identifier,
          |  format_pattern,
          |  country_codes,
          |  separator,
          |  attributes,
          |  rules,
          |  hidden,
          |  max_length,
          |  min_length,
          |  show_member_columns,
          |  decimal_digits
          |  )
          |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, $ordering, ?, ?, ?, ?, ?, ?, ?::json, ?::json, ?, ?, ?, ?, ?)
          |  RETURNING column_id, ordering
          |""".stripMargin
    }

    val rules = createColumn match {
      case createStatusColumn: CreateStatusColumn => createStatusColumn.rules.encode()
      case _ => "[]"
    }

    val countryCodes = createColumn.languageType match {
      case MultiCountry(codes) => Some(codes.codes)
      case _ => None
    }

    val attributes = createColumn.attributes.map(atts => atts.encode()).getOrElse("{}")

    val maxLength = createColumn.maxLength match {
      case None => null
      case Some(num) => num
    }

    val minLength = createColumn.minLength match {
      case None => null
      case Some(num) => num
    }

    def insertColumn(t: DbTransaction): Future[(DbTransaction, CreatedColumnInformation)] = {
      for {
        t <- t
          .selectSingleValue[Long](
            "SELECT COUNT(*) FROM system_columns WHERE table_id = ? AND user_column_name = ?",
            Json.arr(tableId, createColumn.name)
          )
          .flatMap({
            case (t, count) =>
              if (count > 0) {
                Future.failed(ShouldBeUniqueException("Column name should be unique for each table", "column"))
              } else {
                Future.successful(t)
              }
          })

        (t, result) <- createColumn.ordering match {
          case None =>
            t.query(
              insertStatement(tableId, s"currval('system_columns_column_id_table_$tableId')"),
              Json.arr(
                tableId,
                createColumn.kind.name,
                createColumn.name,
                linkId.orNull,
                createColumn.languageType.toString,
                createColumn.identifier,
                formatPattern.orNull,
                countryCodes.map(f => Json.arr(f*)).orNull,
                createColumn.separator,
                attributes,
                rules,
                createColumn.hidden,
                maxLength,
                minLength,
                showMemberColumns,
                createColumn.decimalDigits.orNull
              )
            )
          case Some(ord) =>
            t.query(
              insertStatement(tableId, "?"),
              Json.arr(
                tableId,
                createColumn.kind.name,
                createColumn.name,
                ord,
                linkId.orNull,
                createColumn.languageType.toString,
                createColumn.identifier,
                formatPattern.orNull,
                countryCodes.map(f => Json.arr(f*)).orNull,
                createColumn.separator,
                attributes,
                rules,
                createColumn.hidden,
                maxLength,
                minLength,
                showMemberColumns,
                createColumn.decimalDigits.orNull
              )
            )
        }
      } yield {
        val resultRow = insertNotNull(result).head
        (t, CreatedColumnInformation(tableId, resultRow.getLong(0), resultRow.getLong(1)))
      }
    }

    def insertColumnLang(
        t: DbTransaction,
        displayInfos: ColumnDisplayInfos
    ): Future[(DbTransaction, Seq[DisplayInfo])] = {
      if (displayInfos.nonEmpty) {
        val (statement, binds) = displayInfos.createSql
        for {
          (t, _) <- t.query(statement, Json.arr(binds*))
        } yield (t, displayInfos.entries)
      } else {
        Future.successful((t, List()))
      }
    }

    for {
      (t, result) <- insertColumn(t)
      (t, _) <- insertColumnLang(t, ColumnDisplayInfos(tableId, result.columnId, createColumn.displayInfos))
    } yield (t, result.copy(displayInfos = createColumn.displayInfos))
  }

  def retrieveDependentGroupColumn(tableId: TableId, columnId: ColumnId): Future[Seq[DependentColumnInformation]] = {
    val select =
      s"""
         |SELECT
         |  d.table_id,
         |  d.column_id,
         |  d.column_type,
         |  d.identifier
         |FROM system_columns d JOIN system_column_groups g ON (d.table_id = g.table_id AND d.column_id = g.group_column_id)
         |WHERE d.table_id = ? AND g.grouped_column_id = ?""".stripMargin

    for {
      dependentGroupColumns <- connection.query(select, Json.arr(tableId, columnId))

      dependentGroupColumnInformation = resultObjectToJsonArray(dependentGroupColumns)
        .map(arr => {
          val tableId = arr.get[TableId](0)
          val columnId = arr.get[ColumnId](1)
          val kind = TableauxDbType(arr.get[String](2))
          val identifier = arr.get[Boolean](3)

          DependentColumnInformation(tableId, columnId, kind, identifier, Seq.empty)
        })
    } yield dependentGroupColumnInformation
  }

  def retrieveDependencies(tableId: TableId, depth: Int = MAX_DEPTH): Future[Seq[DependentColumnInformation]] = {

    val select =
      s"""
         |SELECT
         |  d.table_id,
         |  d.column_id,
         |  d.column_type,
         |  d.identifier,
         |  json_agg(g.group_column_id) AS group_column_ids
         |FROM system_link_table l JOIN system_columns d ON (l.link_id = d.link_id) LEFT JOIN system_column_groups g ON (d.table_id = g.table_id AND d.column_id = g.grouped_column_id)
         |WHERE (l.table_id_1 = ? OR l.table_id_2 = ?) AND d.table_id != ?
         |GROUP BY d.table_id, d.column_id
         |ORDER BY d.table_id, d.column_id""".stripMargin

    for {
      dependentColumns <- connection.query(select, Json.arr(tableId, tableId, tableId))

      dependentColumnInformation = resultObjectToJsonArray(dependentColumns)
        .map(mapRowToDependentColumnInformation)

      recursiveDependentColumnInformation <- dependentColumnInformation.foldLeft(
        Future.successful(dependentColumnInformation)
      )({
        case (dependentColumnInformationFuture, dependentColumn) =>
          for {
            dependentColumnInformation <- dependentColumnInformationFuture

            resultSeq <-
              if (dependentColumn.identifier) {
                if (depth > 0) {
                  retrieveDependencies(dependentColumn.tableId, depth - 1)
                } else {
                  Future.failed(DatabaseException("Link is too deep. Check schema.", "link-depth"))
                }
              } else {
                Future.successful(Seq.empty)
              }
          } yield (dependentColumnInformation ++ resultSeq).distinct
      })
    } yield recursiveDependentColumnInformation
  }

  private def mapRowToDependentColumnInformation(row: JsonArray): DependentColumnInformation = {

    val tableId = row.get[TableId](0)
    val columnId = row.get[ColumnId](1)
    val kind = TableauxDbType(row.get[String](2))
    val identifier = row.get[Boolean](3)
    val groupColumnIds = Option(row.get[String](4))
      .map(str => new JsonArray(str).asScala.map(_.asInstanceOf[Int].toLong).toSeq)
      .getOrElse(Seq.empty[ColumnId])

    DependentColumnInformation(tableId, columnId, kind, identifier, groupColumnIds)
  }

  def retrieveDependentLinks(tableId: TableId): Future[Seq[(LinkId, LinkDirection)]] = {
    val select =
      s"""
         |SELECT
         |  l.link_id,
         |  l.table_id_1,
         |  l.table_id_2,
         |  l.cardinality_1,
         |  l.cardinality_2,
         |  l.delete_cascade,
         |  l.archive_cascade,
         |  l.final_cascade,
         |  COUNT(c.*) > 1 AS bidirectional
         |FROM
         |  system_link_table l
         |  LEFT JOIN system_columns c ON (l.link_id = c.link_id)
         |WHERE (table_id_1 = ? OR table_id_2 = ?)
         |GROUP BY l.link_id, l.table_id_1, l.table_id_2""".stripMargin

    for {
      result <- connection.query(select, Json.arr(tableId, tableId))
    } yield {
      resultObjectToJsonArray(result)
        .map({ row =>
          {
            val linkId = row.get[LinkId](0)
            val tableId1 = row.get[TableId](1)
            val tableId2 = row.get[TableId](2)
            val cardinality1 = row.get[Int](3)
            val cardinality2 = row.get[Int](4)
            val deleteCascade = row.get[Boolean](5)
            val archiveCascade = row.get[Boolean](6)
            val finalCascade = row.get[Boolean](7)
            val bidirectional = row.get[Boolean](8)

            val result = (
              linkId,
              LinkDirection(
                tableId,
                tableId1,
                tableId2,
                cardinality1,
                cardinality2,
                deleteCascade,
                archiveCascade,
                finalCascade
              ),
              bidirectional
            )

            logger.info(s"Dependent Links $result")
            result
          }
        })
        .filter({
          case (_, LeftToRight(from, to, _), false) if from == to => true // self link
          case (_, _: LeftToRight, true) => true
          case (_, _: LeftToRight, false) => false
          case (_, _: RightToLeft, _) => true
        })
        .map({
          case (linkId, linkDirection, _) => (linkId, linkDirection)
        })
    }
  }

  def retrieve(table: Table, columnId: ColumnId)(implicit user: TableauxUser): Future[ColumnType[?]] = {
    columnId match {
      case 0 =>
        // Column zero could only be a concat column.
        // We need to retrieve all columns, because
        // only then the ConcatColumn is generated.
        retrieveAll(table).flatMap({
          case Seq(concatColumn: ConcatColumn, _*) =>
            Future.successful(concatColumn)
          case _ =>
            Future.failed(
              NotFoundInDatabaseException(
                s"Either no columns or no ConcatColumn found for table ${table.id}",
                "select"
              )
            )
        })
      case _ =>
        retrieveOne(table, columnId, MAX_DEPTH)
    }
  }

  val baseColumnProjection =
    s"""
       |  column_id,
       |  user_column_name,
       |  column_type,
       |  ordering,
       |  multilanguage,
       |  identifier,
       |  separator,
       |  attributes,
       |  rules,
       |  array_to_json(country_codes),
       |  (
       |    SELECT json_agg(group_column_id) FROM system_column_groups
       |    WHERE table_id = c.table_id AND grouped_column_id = c.column_id
       |  ) AS group_column_ids,
       |  format_pattern,
       |  hidden,
       |  max_length,
       |  min_length,
       |  show_member_columns,
       |  decimal_digits,
       |  (
       |    SELECT json_agg(json_build_object('tableId', origin_table_id, 'columnId', origin_column_id))
       |    FROM system_union_column
       |    WHERE table_id = c.table_id AND column_id = c.column_id
       |  ) AS origin_columns
       |""".stripMargin

  private def retrieveOne(table: Table, columnId: ColumnId, depth: Int)(
      implicit user: TableauxUser
  ): Future[ColumnType[?]] = {
    val select =
      s"""
         |SELECT
         |  $baseColumnProjection
         |FROM system_columns c
         |WHERE
         |  table_id = ? AND
         |  column_id = ?""".stripMargin

    for {
      result <- connection.query(select, Json.arr(table.id, columnId))
      row = selectNotNull(result).head

      mappedColumn <- mapRowResultToColumnType(table, row, depth).flatMap({
        case g: GroupColumn =>
          // if requested column is a GroupColumn we need to get all columns
          // ... because only retrieveColumns can handle GroupColumns
          // TODO: performance: optimize this query if we have multiple GroupColumns
          retrieveAll(table)
            .map(_.find(_.id == g.id).get)

        case column =>
          Future.successful(column)
      })
    } yield mappedColumn
  }

  def retrieveAll(table: Table)(implicit user: TableauxUser): Future[Seq[ColumnType[?]]] =
    retrieveColumns(table, MAX_DEPTH, identifiersOnly = false)

  private def retrieveColumns(table: Table, depth: Int, identifiersOnly: Boolean)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {

    /**
      * Convert the column to the actual GroupColumn (fill with values we can't get from the initial query)
      */
    def fillGroupColumn(mappedColumns: Seq[ColumnType[?]], g: GroupColumn): GroupColumn = {
      val groupedColumns = mappedColumns.filter(_.columnInformation.groupColumnIds.contains(g.id))
      GroupColumn(g.columnInformation, groupedColumns, g.formatPattern, g.showMemberColumns)
    }

    /**
      * Convert the column to the actual UnionColumn (fill with values we can't get from the initial query)
      */
    def fillUnionColumn(originTableCache: Map[TableId, Seq[ColumnType[?]]], u: UnionColumn): UnionColumn = {
      val tableToColumnMap = u.originColumns.tableId2ColumnId.map({
        case (originTableId, originColumnId) =>
          val originColumns = originTableCache(originTableId)
          val matchingOriginColumnOpt = originColumns.find(_.id == originColumnId)
          val matchingOriginColumn = matchingOriginColumnOpt match {
            case Some(originColumn) => originColumn
            case None =>
              throw DatabaseException(
                s"Origin column '${originColumnId}' not found in table '$originTableId' for UnionColumn",
                "missing-origin-column"
              )
          }
          (originTableId, matchingOriginColumn)
      })

      val filledOriginColumns = OriginColumns(u.originColumns.tableId2ColumnId, tableToColumnMap)
      UnionColumn(u.kind, u.languageType, u.columnInformation, filledOriginColumns)
    }

    def getOriginTableCache(mappedColumns: Seq[ColumnType[?]]): Future[Map[TableId, Seq[ColumnType[?]]]] = {
      // Pre-fetch all required origin tables and their columns for UnionColumns
      // Build a cache: Map[TableId, Seq[ColumnType[_]]] to avoid fetching the same table multiple times
      val originTableIds = mappedColumns
        .collect({ case u: UnionColumn => u.originColumns.tableId2ColumnId.keys })
        .flatten
        .toSet

      Future.sequence(
        originTableIds.map(tableId =>
          for {
            originTable <- tableStruc.retrieve(tableId)
            originColumns <- retrieveAll(originTable)
          } yield (tableId, originColumns)
        )
      ).map(_.toMap)
    }

    for {
      result <- connection.query(generateRetrieveColumnsQuery(identifiersOnly), Json.arr(table.id))
      mappedColumns <- Future.sequence(resultObjectToJsonArray(result)
        .map(mapRowResultToColumnType(table, _, depth)))
      originTableCache <- getOriginTableCache(mappedColumns)
      filledColumns = mappedColumns.map({
        case g: GroupColumn => fillGroupColumn(mappedColumns, g)
        case u: UnionColumn => fillUnionColumn(originTableCache, u)
        case other => other
      })
    } yield prependConcatColumnIfNecessary(table, filledColumns)
  }

  private def generateRetrieveColumnsQuery(identifiersOnly: Boolean): String = {
    val identifierFilter =
      if (identifiersOnly) {
        // select either identifier column and/or
        // ... grouped columns if GroupColumn is an identifier
        """
          |AND (identifier = TRUE OR
          |(
          | SELECT COUNT(*)
          | FROM
          | system_columns sc
          | LEFT JOIN system_column_groups g
          |   ON (sc.table_id = g.table_id AND sc.column_id = g.grouped_column_id)
          | LEFT JOIN system_columns sc2
          |   ON (sc2.table_id = g.table_id AND sc2.column_id = g.group_column_id)
          | WHERE sc2.identifier = TRUE AND sc.column_id = c.column_id AND sc.table_id = c.table_id
          |) > 0)""".stripMargin
      } else {
        ""
      }

    s"""
       |SELECT
       |  $baseColumnProjection
       |FROM system_columns c
       |WHERE
       |  table_id = ?
       |  $identifierFilter
       |ORDER BY ordering, column_id""".stripMargin
  }

  private def retrieveIdentifiers(table: Table, depth: Int)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {
    for {
      // we need to retrieve identifiers only otherwise we will end up in a infinite loop
      columns <- retrieveColumns(table, depth, identifiersOnly = true)
    } yield {
      val identifierColumns = columns.filter(_.identifier)

      if (identifierColumns.isEmpty) {
        throw DatabaseException("Link can not point to table without identifier(s).", "missing-identifier")
      } else {
        identifierColumns
      }
    }
  }

  private def prependConcatColumnIfNecessary(table: Table, columns: Seq[ColumnType[?]])(
      implicit user: TableauxUser
  ): Seq[ColumnType[?]] = {
    val identifierColumns = columns.filter(_.identifier)

    // a concat column is needed whenever there are multiple identifier columns (to preserve
    // their order behind one virtual column) or when the single identifier column is a link
    // column, since a link's "to" column must resolve to a value that can be fetched/concatenated
    // (see mapLinkColumn / fetchConcatValuesForLinkedRows)
    val hasSingleIdentifier = identifierColumns.size == 1
    val hasMultipleIdentifiers = identifierColumns.size >= 2
    val hasSingleLinkIdentifier = hasSingleIdentifier && identifierColumns.head.isInstanceOf[LinkColumn]
    val needsConcatColumn = hasMultipleIdentifiers || hasSingleLinkIdentifier

    if (needsConcatColumn) {
      columns.+:(ConcatColumn(ConcatColumnInformation(table), identifierColumns, table.concatFormatPattern))
    } else if (hasSingleIdentifier) {
      // in case of one (non-link) identifier column we don't get a concat column
      // but the identifier column will be the first
      columns.sortBy(_.identifier)(Ordering[Boolean].reverse)
    } else {
      // no identifier -> return columns
      columns
    }
  }

  private def mapColumn(
      depth: Int,
      kind: TableauxDbType,
      languageType: LanguageType,
      columnInformation: ColumnInformation,
      formatPattern: Option[String],
      rules: JsonArray,
      showMemberColumns: Boolean
  )(implicit user: TableauxUser): Future[ColumnType[?]] = {
    kind match {
      case AttachmentType => Future(AttachmentColumn(columnInformation))
      case StatusType => mapStatusColumn(columnInformation, rules)
      case LinkType => mapLinkColumn(depth, columnInformation, formatPattern)
      // placeholder for now, grouped columns will be filled in later
      case GroupType => Future(GroupColumn(columnInformation, Seq.empty, formatPattern, showMemberColumns))
      case _ => Future(SimpleValueColumn(kind, languageType, columnInformation))
    }
  }

  private def mapUnionColumn(
      kind: TableauxDbType,
      languageType: LanguageType,
      columnInformation: ColumnInformation,
      originColumns: Option[OriginColumns]
  )(implicit user: TableauxUser): Future[ColumnType[?]] = {
    Future(UnionColumn(
      kind,
      languageType,
      columnInformation,
      // we have no origin columns in the first union table column (OriginTableColumn)
      originColumns.getOrElse(OriginColumns(Map()))
    ))
  }

  private def mapStatusColumn(columnInformation: ColumnInformation, rules: JsonArray)(
      implicit user: TableauxUser
  ): Future[StatusColumn] = {
    for {
      columns <- retrieveAndValidateDependentStatusColumns(rules, columnInformation.table)
      statusColumn = StatusColumn(columnInformation, rules, columns)
    } yield statusColumn
  }

  def retrieveAndValidateDependentStatusColumns(rules: JsonArray, table: Table)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[?]]] = {

    def calcDependentColumnValuesFromValues(values: JsonArray): Seq[(ColumnId, Any)] = {

      asSeqOf[JsonObject](values)
        .flatMap(value => {
          if (value.containsKey("values")) {
            val newValues = value.getJsonArray("values")
            calcDependentColumnValuesFromValues(newValues)
          } else {
            val columnId = value.getLong("column").asInstanceOf[ColumnId]
            Seq(columnId -> value.getValue("value"))
          }
        })
    }

    val dependentColumnValues = asSeqOf[JsonObject](rules)
      .flatMap(json => {
        val values = json.getJsonObject("conditions").getJsonArray("values")
        calcDependentColumnValuesFromValues(values)
      })

    val valueTypeMap: Map[ColumnId, Seq[Any]] =
      dependentColumnValues
        .groupBy({ case (columnId, _) => columnId })
        .view
        .mapValues(_.map({ case (_, value) => value }))
        .toMap

    val dependentColumnIds = dependentColumnValues.map({ case (columnId, _) => columnId }).distinct

    for {
      columns <- Future.sequence(dependentColumnIds.map(id =>
        retrieveOne(table, id, 1).recover({
          case _ => throw new ColumnNotFoundException(s"Column with id $id not found")
        })
      ))
      // validate column types and languagetype
      _ = columns.foreach(column => {
        if (!StatusColumn.validColumnTypes.contains(column.kind)) {
          throw new WrongStatusColumnKindException(column, StatusColumn.validColumnTypes)
        }
        column.languageType match {
          case LanguageNeutral => {}
          case _ => throw new WrongLanguageTypeException(column, LanguageNeutral)
        }

        val (checkForExpectedValueType, expectedType): (Any => Boolean, String) = column.kind match {
          case TextType => ((valueToCompare: Any) => valueToCompare.isInstanceOf[String], "String")
          case ShortTextType => ((valueToCompare: Any) => valueToCompare.isInstanceOf[String], "String")
          case RichTextType => ((valueToCompare: Any) => valueToCompare.isInstanceOf[String], "String")
          case NumericType => ((valueToCompare: Any) => valueToCompare.isInstanceOf[Number], "Number")
          case BooleanType => ((valueToCompare: Any) => valueToCompare.isInstanceOf[Boolean], "Boolean")
          case _ => throw new WrongStatusColumnKindException(column, StatusColumn.validColumnTypes)
        }

        valueTypeMap(column.id).foreach(value => {
          if (!checkForExpectedValueType(value)) {
            throw new WrongStatusConditionTypeException(column, value.getClass().toString, expectedType)
          }
        })

      })
    } yield columns
  }

  private def mapLinkColumn(depth: Int, columnInformation: ColumnInformation, formatPattern: Option[String])(
      implicit user: TableauxUser
  ): Future[LinkColumn] = {
    for {
      (linkId, linkDirection, toTable, linkAttributes) <-
        retrieveLinkInformation(columnInformation.table, columnInformation.id)

      foreignColumns <- {
        if (depth > 0) {
          retrieveIdentifiers(toTable, depth - 1)
        } else {
          Future.failed(DatabaseException("Link is too deep. Check schema.", "link-depth"))
        }
      }

      // Link should point at ConcatColumn if defined
      // Otherwise use the first column which should be an identifier
      // If no identifier is defined, use the first column. Period.
      toColumnOpt = foreignColumns.headOption
    } yield {
      if (toColumnOpt.isEmpty) {
        throw NotFoundInDatabaseException(s"Link points at table ${toTable.id} without columns", "no-columns")
      }

      val toColumn = toColumnOpt.get
      LinkColumn(columnInformation, toColumn, linkId, linkDirection, linkAttributes, formatPattern)
    }
  }

  private def mapRowResultToColumnType(table: Table, row: JsonArray, depth: Int)(
      implicit user: TableauxUser
  ): Future[ColumnType[?]] = {
    val columnId = row.get[ColumnId](0)
    val columnName = row.get[String](1)
    val kind = TableauxDbType(row.get[String](2))
    val ordering = row.get[Ordering](3)
    val identifier = row.get[Boolean](5)
    val separator = row.get[Boolean](6)
    val attributes = new JsonObject(row.get[String](7))
    val rules = new JsonArray(row.get[String](8))

    val languageType = LanguageType(Option(row.get[String](4))) match {
      case LanguageNeutral => LanguageNeutral
      case MultiLanguage => MultiLanguage
      case c: MultiCountry =>
        val codes = Option(row.get[String](9))
          .map(str => new JsonArray(str).asScala.map({ case code: String => code }).toSeq)
          .getOrElse(Seq.empty[String])

        MultiCountry(CountryCodes(codes))
    }

    val groupColumnIds = Option(row.get[String](10))
      .map(str => new JsonArray(str).asScala.map(_.asInstanceOf[Int].toLong).toSeq)
      .getOrElse(Seq.empty[ColumnId])

    val formatPattern = Option(row.get[String](11))
    val hidden = row.get[Boolean](12)
    val maxLength = Option(row.get[Int](13))
    val minLength = Option(row.get[Int](14))
    val showMemberColumns = row.get[Boolean](15)
    val decimalDigits = Option(row.get[Int](16))
    val originColumns = Option(row.get[String](17))
      .map(str => OriginColumns.parseJson(new JsonArray(str)))

    val getBasicColumnInfo = BasicColumnInformation(
      table,
      columnId,
      columnName,
      ordering,
      identifier,
      _: Seq[DisplayInfo],
      groupColumnIds,
      separator,
      attributes,
      hidden,
      maxLength,
      minLength,
      decimalDigits
    )

    for {
      displayInfoSeq <- retrieveDisplayInfo(table, columnId)
      columnInfo = getBasicColumnInfo(displayInfoSeq)
      column <- table.tableType match {
        case UnionTable => mapUnionColumn(kind, languageType, columnInfo, originColumns)
        case _ => mapColumn(depth, kind, languageType, columnInfo, formatPattern, rules, showMemberColumns)
      }
    } yield column

  }

  private def retrieveDisplayInfo(table: Table, columnId: ColumnId): Future[Seq[DisplayInfo]] = {
    val selectLang =
      s"""SELECT langtag, name, description
         | FROM system_columns_lang
         | WHERE table_id = ? AND column_id = ?""".stripMargin

    for {
      resultLang <- connection.query(selectLang, Json.arr(table.id, columnId))
      displayInfos = resultObjectToJsonArray(resultLang).flatMap({ arr =>
        val langtag = arr.getString(0)
        val name = Option(arr.getString(1))
        val description = Option(arr.getString(2))

        if (name.isDefined || description.isDefined) {
          Seq(DisplayInfos.fromString(langtag, name.orNull, description.orNull))
        } else {
          Seq.empty
        }
      })
    } yield displayInfos
  }

  def retrieveLinkInformation(fromTable: Table, columnId: ColumnId)(
      implicit user: TableauxUser
  ): Future[(LinkId, LinkDirection, Table, Seq[LinkAttributeDefinition])] = {
    for {
      result <- connection.query(
        """
          |SELECT
          | table_id_1,
          | table_id_2,
          | link_id,
          | cardinality_1,
          | cardinality_2,
          | delete_cascade,
          | archive_cascade,
          | final_cascade,
          | attributes
          |FROM system_link_table
          |WHERE link_id = (
          |  SELECT link_id
          |  FROM system_columns
          |  WHERE table_id = ? AND column_id = ?
          |)""".stripMargin,
        Json.arr(fromTable.id, columnId)
      )

      (linkId, linkDirection, linkAttributes) = {
        val res = selectNotNull(result).head

        val table1 = res.getLong(0).longValue()
        val table2 = res.getLong(1).longValue()
        val linkId = res.getLong(2).longValue()
        val cardinality1 = res.getLong(3).intValue()
        val cardinality2 = res.getLong(4).intValue()
        val deleteCascade = res.getBoolean(5)
        val archiveCascade = res.getBoolean(6)
        val finalCascade = res.getBoolean(7)
        val linkAttributes = Option(res.getString(8))
          .map(str => LinkAttributeDefinition.seqFromJson(new JsonArray(str)))
          .getOrElse(Seq.empty)

        (
          linkId,
          LinkDirection(
            fromTable.id,
            table1,
            table2,
            cardinality1,
            cardinality2,
            deleteCascade,
            archiveCascade,
            finalCascade
          ),
          linkAttributes
        )
      }

      toTable <- tableStruc.retrieve(linkDirection.to, isInternalCall = true)

    } yield (linkId, linkDirection, toTable, linkAttributes)
  }

  def deleteLinkBothDirections(table: Table, columnId: ColumnId)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    delete(table, columnId, bothDirections = true, checkForLastColumn = false)
  }

  def delete(table: Table, columnId: ColumnId)(implicit user: TableauxUser): Future[Unit] =
    delete(table, columnId, bothDirections = false, checkForLastColumn = true)

  protected def delete(
      table: Table,
      columnId: ColumnId,
      bothDirections: Boolean,
      checkForLastColumn: Boolean = true
  )(implicit user: TableauxUser): Future[Unit] = {

    // Retrieve all filter for columnId and check if columns is not empty
    // If columns is empty last column would be deleted => error
    for {
      columns <- retrieveAll(table)
        .filter(_.nonEmpty)
        .recoverWith({
          case _: NoSuchElementException =>
            Future.failed(NotFoundInDatabaseException("No column found at all", "no-column-found"))
        })

      _ <-
        if (checkForLastColumn) {
          Future
            .successful(columns)
            .filter(!_.forall(_.id == columnId))
            .recoverWith({
              case _: NoSuchElementException =>
                Future.failed(DatabaseException("Last column can't be deleted", "delete-last-column"))
            })
        } else {
          Future.successful(columns)
        }

      column = columns
        .find(_.id == columnId)
        .getOrElse(
          throw NotFoundInDatabaseException("Column can't be deleted because it doesn't exist.", "delete-non-existing")
        )

      _ = checkForStatusColumnDependency(columnId, columns, "deleted")

      _ <- {
        column match {
          case c: ConcatColumn => Future.failed(DatabaseException("ConcatColumn can't be deleted", "delete-concat"))
          case c: LinkColumn => deleteLink(c, bothDirections)
          case c: AttachmentColumn => deleteAttachment(c)
          case c: UnionColumn if c.kind == OriginTableType =>
            Future.failed(DatabaseException("Column origintable can't be deleted", "delete-column-origintable"))
          case c: ColumnType[_] => deleteSimpleColumn(c)
        }
      }
    } yield ()
  }

  private def checkForStatusColumnDependency(
      columnId: ColumnId,
      columns: Seq[ColumnType[?]],
      actionErrorMessage: String
  ): Unit = {
    columns
      .filter(column => column.kind == StatusType)
      .foreach(column => {
        if (column.asInstanceOf[StatusColumn].columns.map(col => col.id).contains(columnId)) {
          throw new HasStatusColumnDependencyException(
            s"Column can't be ${actionErrorMessage} because Column with id ${column.id} has dependency on this column. Remove Rules from Column ${column.name} with id: ${column.id} containing  column with id: ${columnId} "
          )
        }
      })
  }

  private def deleteLink(column: LinkColumn, bothDirections: Boolean): Future[Unit] = {
    val tableId = column.table.id
    val columnId = column.id

    for {
      t <- connection.begin()

      (t, result) <- t.query(
        "SELECT link_id FROM system_columns WHERE column_id = ? AND table_id = ?",
        Json.arr(column.id, column.table.id)
      )
      linkId = selectCheckSize(result, 1).head.get[Long](0)

      (t, result) <- t.query("SELECT COUNT(*) = 1 FROM system_columns WHERE link_id = ?", Json.arr(linkId))
      unidirectional = selectCheckSize(result, 1).head.getBoolean(0).booleanValue()

      (t, _) <- {
        // We only want to delete both directions
        // when we delete one of the two tables
        // which are linked together.
        // ColumnModel.deleteLink() with both = true
        // is called by StructureController.deleteTable().
        val deleteFuture =
          if (bothDirections) {
            for {
              (t, _) <- t.query(
                s"""
                   |DELETE FROM user_table_annotations_$tableId ua
                   |WHERE EXISTS (
                   |SELECT 1 FROM system_columns c
                   |WHERE
                   |  c.link_id = ? AND
                   |  c.table_id = ? AND
                   |  ua.column_id = c.column_id
                   |)""".stripMargin,
                Json.arr(linkId, tableId)
              )
              (t, _) <- t.query("DELETE FROM system_columns WHERE link_id = ?", Json.arr(linkId))
            } yield t
          } else {
            for {
              (t) <- deleteSystemColumn(t, tableId, columnId)
              (t) <- deleteAnnotations(t, tableId, columnId)
            } yield t
          }

        deleteFuture.flatMap({ t =>
          if (unidirectional || bothDirections) {
            // drop link_table if link is unidirectional or
            // both directions where forcefully deleted
            for {
              (t, _) <- t.query(s"DROP TABLE IF EXISTS link_table_$linkId")
              (t, _) <- t.query(s"DELETE FROM system_link_table WHERE link_id = ?", Json.arr(linkId))
            } yield (t, Json.obj())
          } else {
            Future.successful((t, Json.obj()))
          }
        })
      }

      _ <- t.commit()
    } yield ()
  }

  private def deleteAttachment(column: AttachmentColumn): Future[Unit] = {
    val tableId = column.table.id
    val columnId = column.id

    for {
      t <- connection.begin()

      (t, _) <- t
        .query("DELETE FROM system_attachment WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))

      (t) <- deleteSystemColumn(t, tableId, columnId)
      (t) <- deleteAnnotations(t, tableId, columnId)

      _ <- t.commit()
    } yield ()
  }

  private def deleteSimpleColumn(column: ColumnType[?]): Future[Unit] = {
    val tableId = column.table.id
    val columnId = column.id

    for {
      t <- connection.begin()

      (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId")
      (t, _) <- t.query(s"ALTER TABLE user_table_lang_$tableId DROP COLUMN IF EXISTS column_$columnId")

      (t) <- deleteSystemColumn(t, tableId, columnId)
      (t) <- deleteAnnotations(t, tableId, columnId)

      _ <- t.commit()
    } yield ()
  }

  private def deleteUnionSimpleColumn(column: ColumnType[?]): Future[Unit] = {
    val tableId = column.table.id
    val columnId = column.id

    for {
      t <- connection.begin()
      (t) <- deleteSystemColumn(t, tableId, columnId)
      _ <- t.commit()
    } yield ()
  }

  private def deleteSystemColumn(
      t: DbTransaction,
      tableId: TableId,
      columnId: ColumnId
  ): Future[DbTransaction] = {
    for {
      (t, _) <- t
        .query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
        .map({ case (t, json) => (t, deleteNotNull(json)) })
    } yield t
  }

  private def deleteAnnotations(
      t: DbTransaction,
      tableId: TableId,
      columnId: ColumnId
  ): Future[DbTransaction] = {
    for {
      (t, _) <- t.query(s"DELETE FROM user_table_annotations_$tableId WHERE column_id = ?", Json.arr(columnId))
    } yield t
  }

  private def getUpdateQueryFor(
      columnName: String,
      cast: String = ""
  ): String = s"UPDATE system_columns SET $columnName = ?$cast WHERE table_id = ? AND column_id = ?"

  // Reshapes existing attribute values (position 0, the only slot while linkAttributes is capped at 1) to match a
  // multilanguage flip, before any kind cast runs on top. There's no cast for this - it's a structural change - so
  // false -> true duplicates the scalar under every table langtag, and true -> false collapses to the first langtag
  // (in configured priority order) that actually has a non-null value, discarding the rest.
  private def reshapeLinkAttributeValues(
      t: DbTransaction,
      table: Table,
      linkTable: String,
      oldDefinition: LinkAttributeDefinition,
      newDefinition: LinkAttributeDefinition
  ): Future[(DbTransaction, JsonObject)] = {
    if (oldDefinition.multilanguage == newDefinition.multilanguage) {
      Future.successful((t, Json.obj()))
    } else {
      for {
        langtags <- table.langtags.map(Future.successful).getOrElse(tableStruc.retrieveGlobalLangtags())

        result <-
          if (newDefinition.multilanguage) {
            // Postgres can't infer a bare `?` placeholder's type from a variadic "any" function like
            // jsonb_build_object - it needs an explicit cast, or every prepared execution fails with
            // "could not determine data type of parameter $1".
            val pairs = langtags.map(_ => "?::text, attributes->0").mkString(", ")
            t.query(
              s"""|UPDATE $linkTable
                  |SET attributes = jsonb_set(attributes, '{0}', jsonb_build_object($pairs))
                  |WHERE attributes IS NOT NULL AND attributes->0 IS NOT NULL""".stripMargin,
              Json.arr(langtags*)
            )
          } else {
            val coalesceParts = (langtags.map(_ => "attributes->0->?::text") :+ "'null'::jsonb").mkString(", ")
            t.query(
              s"""|UPDATE $linkTable
                  |SET attributes = jsonb_set(attributes, '{0}', COALESCE($coalesceParts))
                  |WHERE attributes IS NOT NULL AND attributes->0 IS NOT NULL""".stripMargin,
              Json.arr(langtags*)
            )
          }
      } yield result
    }
  }

  // Casts existing attribute values (position 0) to a new kind, all-or-nothing - a single value anywhere that can't
  // cast fails the whole UPDATE, which (combined with the caller's rollbackAndFail) rolls back the entire change,
  // exactly mirroring how a plain column's kind change behaves today (ALTER COLUMN ... USING ...::type).
  private def castLinkAttributeValues(
      t: DbTransaction,
      linkTable: String,
      oldDefinition: LinkAttributeDefinition,
      newDefinition: LinkAttributeDefinition
  ): Future[(DbTransaction, JsonObject)] = {
    if (oldDefinition.kind == newDefinition.kind) {
      Future.successful((t, Json.obj()))
    } else if (!newDefinition.multilanguage) {
      t.query(
        s"""|UPDATE $linkTable
            |SET attributes = jsonb_set(attributes, '{0}', to_jsonb((attributes->>0)::${newDefinition.kind.toDbType}))
            |WHERE attributes IS NOT NULL AND attributes->0 IS NOT NULL""".stripMargin
      )
    } else {
      t.query(
        s"""|UPDATE $linkTable
            |SET attributes = jsonb_set(
            |  attributes, '{0}',
            |  (SELECT jsonb_object_agg(key, to_jsonb(value::${newDefinition.kind.toDbType}))
            |   FROM jsonb_each_text(attributes->0))
            |)
            |WHERE attributes IS NOT NULL AND attributes->0 IS NOT NULL""".stripMargin
      )
    }
  }

  // Applies a linkAttributes definition change to system_link_table plus, when needed, migrates existing values
  // already stored on link_table_<linkId>. Diffing is by name (max-1 keeps this simple): no old + new = pure add
  // (nothing to migrate); old + no new, or a rename (different name) = wipe stored values, since there's no
  // continuity contract once the name that referenced them is gone; same name = reshape (multilanguage) then
  // cast (kind) in place.
  private def updateLinkAttributesDefinition(
      t: DbTransaction,
      table: Table,
      columnId: ColumnId,
      newDefinitions: Seq[LinkAttributeDefinition]
  ): Future[(DbTransaction, JsonObject)] = {
    for {
      (t, linkIdResult) <- t.query(
        "SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?",
        Json.arr(table.id, columnId)
      )
      linkId = selectNotNull(linkIdResult).head.getLong(0).longValue()
      linkTable = s"link_table_$linkId"

      (t, currentResult) <- t.query("SELECT attributes FROM system_link_table WHERE link_id = ?", Json.arr(linkId))
      currentDefinitions = Option(selectNotNull(currentResult).head.getString(0))
        .map(str => LinkAttributeDefinition.seqFromJson(new JsonArray(str)))
        .getOrElse(Seq.empty)

      (t, _) <- (currentDefinitions.headOption, newDefinitions.headOption) match {
        case (Some(oldDef), Some(newDef)) if oldDef.name == newDef.name =>
          for {
            (t, _) <- reshapeLinkAttributeValues(t, table, linkTable, oldDef, newDef)
            (t, result) <- castLinkAttributeValues(t, linkTable, oldDef, newDef)
          } yield (t, result)

        case (Some(_), _) =>
          // pure remove, or renamed to a different name - either way the old values no longer have a definition
          t.query(s"UPDATE $linkTable SET attributes = NULL")

        case (None, _) =>
          // pure add - no existing link rows can have a value yet
          Future.successful((t, Json.obj()))
      }

      (t, result) <- t.query(
        "UPDATE system_link_table SET attributes = ?::jsonb WHERE link_id = ?",
        Json.arr(Json.arr(newDefinitions.map(LinkAttributeDefinition.getJson)*).encode(), linkId)
      )
    } yield (t, result)
  }

  def change(
      table: Table,
      columnId: ColumnId,
      columnName: Option[String],
      ordering: Option[Ordering],
      kind: Option[TableauxDbType],
      identifier: Option[Boolean],
      displayInfos: Option[Seq[DisplayInfo]],
      countryCodes: Option[Seq[String]],
      separator: Option[Boolean],
      attributes: Option[JsonObject],
      rules: Option[JsonArray],
      hidden: Option[Boolean],
      maxLength: Option[Int],
      minLength: Option[Int],
      showMemberColumns: Option[Boolean],
      decimalDigits: Option[Int],
      formatPattern: Option[String],
      linkAttributes: Option[Seq[LinkAttributeDefinition]]
  )(implicit user: TableauxUser): Future[ColumnType[?]] = {
    val tableId = table.id

    def maybeUpdateColumn[VALUE_TYPE](
        t: DbTransaction,
        columnName: String,
        value: Option[VALUE_TYPE],
        trans: VALUE_TYPE => ? = (v: VALUE_TYPE) => v,
        cast: String = ""
    ): Future[(DbTransaction, JsonObject)] = {
      optionToValidFuture(
        value,
        t,
        { (v: VALUE_TYPE) => t.query(getUpdateQueryFor(columnName, cast), Json.arr(trans(v), tableId, columnId)) }
      )
    }

    for {
      t <- connection.begin()

      // change column settings
      (t, resultColumnName) <- maybeUpdateColumn(t, "user_column_name", columnName)
      (t, resultOrdering) <- maybeUpdateColumn(t, "ordering", ordering)
      (t, resultKind) <- maybeUpdateColumn(t, "column_type", kind, (k: TableauxDbType) => k.name)
      (t, resultIdentifier) <- maybeUpdateColumn(t, "identifier", identifier)
      (t, resultSeparator) <- maybeUpdateColumn(t, "separator", separator)
      (t, resultAttributes) <-
        maybeUpdateColumn(t, "attributes", attributes, (a: JsonObject) => a.encode(), "::json")
      (t, resultRules) <- maybeUpdateColumn(t, "rules", rules, (r: JsonArray) => r.encode(), "::json")
      (t, resultCountryCodes) <-
        maybeUpdateColumn(t, "country_codes", countryCodes, (c: Seq[String]) => Json.arr(c*))
      (t, resultHidden) <- maybeUpdateColumn(t, "hidden", hidden)
      (t, resultShowMemberColumns) <- maybeUpdateColumn(t, "show_member_columns", showMemberColumns)
      (t, resultDecimalDigits) <- maybeUpdateColumn(t, "decimal_digits", decimalDigits)
      (t, resultFormatPattern) <- maybeUpdateColumn(t, "format_pattern", formatPattern)

      // cannot use optionToValidFuture here, we need to be able to set these settings to null
      (t, resultMaxLength) <- maxLength match {
        case Some(maxLen) => t.query(getUpdateQueryFor("max_length"), Json.arr(maxLen, tableId, columnId))
        case None => t.query(getUpdateQueryFor("max_length"), Json.arr(null, tableId, columnId))
      }
      (t, resultMinLength) <- minLength match {
        case Some(minLen) => t.query(getUpdateQueryFor("min_length"), Json.arr(minLen, tableId, columnId))
        case None => t.query(getUpdateQueryFor("min_length"), Json.arr(null, tableId, columnId))
      }

      // change display information
      t <- insertOrUpdateColumnLangInfo(t, table.id, columnId, displayInfos)

      // change column kind
      (t, _) <- optionToValidFuture(
        kind,
        t,
        { (k: TableauxDbType) =>
          t.query(
            s"ALTER TABLE user_table_$tableId ALTER COLUMN column_$columnId TYPE ${k.toDbType} USING column_$columnId::${k.toDbType}"
          )

        }
      ).recoverWith(t.rollbackAndFail())

      // change linkAttributes definition, migrating already-stored values (see updateLinkAttributesDefinition)
      (t, _) <- optionToValidFuture(
        linkAttributes,
        t,
        { (newDefinitions: Seq[LinkAttributeDefinition]) =>
          updateLinkAttributesDefinition(t, table, columnId, newDefinitions)
        }
      ).recoverWith(t.rollbackAndFail())

      _ <- Future(
        checkUpdateResults(
          resultColumnName,
          resultOrdering,
          resultKind,
          resultIdentifier,
          resultCountryCodes,
          resultSeparator,
          resultAttributes,
          resultRules,
          resultMaxLength,
          resultMinLength,
          resultShowMemberColumns,
          resultDecimalDigits,
          resultFormatPattern
        )
      )
        .recoverWith(t.rollbackAndFail())

      _ <- t.commit()

      column <- retrieve(table, columnId)
    } yield column
  }

  private def insertOrUpdateColumnLangInfo(
      t: DbTransaction,
      tableId: TableId,
      columnId: ColumnId,
      optDisplayInfos: Option[Seq[DisplayInfo]]
  ): Future[DbTransaction] = {

    optDisplayInfos match {
      case Some(displayInfos) =>
        val dis = ColumnDisplayInfos(tableId, columnId, displayInfos)
        dis.entries.foldLeft(Future.successful(t)) {
          case (future, di) =>
            for {
              t <- future
              (t, select) <- t.query(
                "SELECT COUNT(*) FROM system_columns_lang WHERE table_id = ? AND column_id = ? AND langtag = ?",
                Json.arr(tableId, columnId, di.langtag)
              )
              count = select.getJsonArray("results").getJsonArray(0).getLong(0)
              (statement, binds) =
                if (count > 0) {
                  dis.updateSql(di.langtag)
                } else {
                  dis.insertSql(di.langtag)
                }
              (t, _) <- t.query(statement, Json.arr(binds*))
            } yield t
        }
      case None => Future.successful(t)
    }
  }
}
