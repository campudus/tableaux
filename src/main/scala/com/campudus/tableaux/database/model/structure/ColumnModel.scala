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
import com.campudus.tableaux.helper.JsonUtils.asSeqOf
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.router.auth.permission.TableauxUser
import com.campudus.tableaux.verticles.EventClient
import com.campudus.tableaux.verticles.ValidatorKeys

import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import com.google.common.cache.CacheBuilder
import com.typesafe.scalalogging.LazyLogging
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import scalacache._
import scalacache.guava._
import scalacache.modes.scalaFuture._

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

  implicit val scalaCache: Cache[Object] = GuavaCache(createCache())

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

    builder.build[String, Entry[Object]]
  }

  def removeAllCache(): Future[Unit] = {
    for {
      _ <- removeAll()
    } yield ()
  }

  private def removeCache(tableId: TableId, columnIdOpt: Option[ColumnId]): Future[Unit] = {

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
  ): Future[ColumnType[_]] = {
    cachingF[Future, Object]("retrieve", table.id, columnId)(None)(
      super.retrieve(table, columnId)
    ).asInstanceOf[Future[ColumnType[_]]]
  }

  override def retrieveAll(table: Table)(implicit user: TableauxUser): Future[Seq[ColumnType[_]]] = {
    cachingF[Future, Object]("retrieveAll", table.id)(None)(
      super.retrieveAll(table)
    ).asInstanceOf[Future[Seq[ColumnType[_]]]]
  }

  override def createColumns(table: Table, createColumns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[_]]] = {
    for {
      r <- super.createColumns(table, createColumns)
      _ <- removeCache(table.id, None)
    } yield r
  }

  override def createColumn(table: Table, createColumn: CreateColumn)(
      implicit user: TableauxUser
  ): Future[ColumnType[_]] = {
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
      decimalDigits: Option[Int]
  )(implicit user: TableauxUser): Future[ColumnType[_]] = {
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
          decimalDigits
        )
    } yield r
  }

  override def retrieveAndValidateDependentStatusColumns(rules: JsonArray, table: Table)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[_]]] = {
    super.retrieveAndValidateDependentStatusColumns(rules, table)
  }
}

object ColumnModel extends LazyLogging {

  def isColumnGroupMatchingToFormatPattern(
      formatPattern: Option[String],
      groupedColumns: Seq[ColumnType[_]]
  ): Boolean = {
    val formatVariable = "\\{\\{(\\d+)\\}\\}".r

    formatPattern match {
      case Some(patternString) => {
        val distinctWildcards =
          formatVariable
            .findAllMatchIn(patternString)
            .toSeq
            .flatMap(_.subgroups)
            .distinct
            .map(_.toLong)
            .to[SortedSet]

        val columnIDs = groupedColumns.map(_.id).to[SortedSet]

        logger.info(
          s"Compare distinct wildcards (${distinctWildcards.mkString(", ")}) " +
            s"with columnIDs (${columnIDs.mkString(", ")})"
        )

        distinctWildcards == columnIDs
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
  ): Future[Seq[ColumnType[_]]] = {
    createColumns.foldLeft(Future.successful(Seq.empty[ColumnType[_]])) {
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
  ): Future[ColumnType[_]] = {

    val attributes = createColumn.attributes
    val validator = EventClient(Vertx.currentContext().get.owner())

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
          Future { Unit }
        }
      columnCreate <- createColumn match {
        case simpleColumnInfo: CreateSimpleColumn =>
          createValueColumn(table.id, simpleColumnInfo)
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
                LinkColumn(applyColumnInformation(id, ordering, displayInfos), toCol, linkId, linkDirection)
            })

        case attachmentColumnInfo: CreateAttachmentColumn =>
          createAttachmentColumn(table.id, attachmentColumnInfo)
            .map({
              case CreatedColumnInformation(_, id, ordering, displayInfos) =>
                AttachmentColumn(applyColumnInformation(id, ordering, displayInfos))
            })

        case groupColumnInfo: CreateGroupColumn => {
          println("groupColumnInfo: " + groupColumnInfo)
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
      columnCreate
    }
  }

  private def createStatusColumn(
      table: Table,
      statusColumnInfo: CreateStatusColumn
  )(implicit user: TableauxUser): Future[(Seq[ColumnType[_]], CreatedColumnInformation)] = {
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

    connection.transactional { t =>
      for {
        // retrieve all to-be-grouped columns
        groupedColumns <- retrieveAll(table)
          .map(_.filter(column => cgc.groups.contains(column.id)))

        // do some validation before creating GroupColumn
        _ = {
          if (groupedColumns.size != cgc.groups.size) {
            throw UnprocessableEntityException(
              s"GroupColumn (${cgc.name}) couldn't be created because some columns don't exist"
            )
          }

          if (groupedColumns.exists(_.kind == GroupType)) {
            throw UnprocessableEntityException(
              s"GroupColumn (${cgc.name}) can't contain another GroupColumn"
            )
          }

          if (!isColumnGroupMatchingToFormatPattern(cgc.formatPattern, groupedColumns)) {
            throw UnprocessableEntityException(
              s"GroupColumns (${groupedColumns.map(_.id).mkString(", ")}) don't match to formatPattern " +
                s"${"\"" + cgc.formatPattern.map(_.toString).orNull + "\""}"
            )
          }
        }

        (t, columnInfo) <- insertSystemColumn(t, tableId, cgc, None, cgc.formatPattern, cgc.showMemberColumns)

        // insert group information
        insertPlaceholder = cgc.groups.map(_ => "(?, ?, ?)").mkString(", ")
        (t, _) <- t.query(
          s"INSERT INTO system_column_groups(table_id, group_column_id, grouped_column_id) VALUES $insertPlaceholder",
          Json.arr(cgc.groups.flatMap(Seq(tableId, columnInfo.columnId, _)): _*)
        )
      } yield (t, columnInfo)
    }
  }

  private def createValueColumn(
      tableId: TableId,
      simpleColumnInfo: CreateSimpleColumn
  ): Future[CreatedColumnInformation] = {
    connection.transactional { t =>
      for {
        (t, columnInfo) <- insertSystemColumn(t, tableId, simpleColumnInfo, None, None, false)
        tableSql = simpleColumnInfo.languageType match {
          case MultiLanguage | _: MultiCountry => s"user_table_lang_$tableId"
          case LanguageNeutral => s"user_table_$tableId"
        }

        (t, _) <- simpleColumnInfo.kind match {
          case BooleanType =>
            t.query(s"ALTER TABLE $tableSql ADD column_${columnInfo.columnId} BOOLEAN DEFAULT false")
          case _ =>
            t.query(s"ALTER TABLE $tableSql ADD column_${columnInfo.columnId} ${simpleColumnInfo.kind.toDbType}")
        }
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
  )(implicit user: TableauxUser): Future[(LinkId, ColumnType[_], CreatedColumnInformation)] = {
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
             |  final_cascade
             |) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING link_id""".stripMargin,
          Json.arr(
            tableId,
            linkColumnInfo.toTable,
            linkColumnInfo.constraint.cardinality.from,
            linkColumnInfo.constraint.cardinality.to,
            linkColumnInfo.constraint.deleteCascade,
            linkColumnInfo.constraint.archiveCascade,
            linkColumnInfo.constraint.finalCascade
          )
        )
        linkId = insertNotNull(result).head.get[Long](0)

        // insert link column on source table
        (t, columnInfo) <- insertSystemColumn(t, tableId, linkColumnInfo, Some(linkId), None, false)

        // only add the second link column if tableId != toTableId or singleDirection is false
        t <- {
          if (!linkColumnInfo.singleDirection && tableId != linkColumnInfo.toTable) {
            val copiedLinkColumnInfo = linkColumnInfo.copy(
              name = linkColumnInfo.foreignLinkColumn.name.getOrElse(table.name),
              identifier = false,
              displayInfos = linkColumnInfo.foreignLinkColumn.displayInfos.getOrElse({
                table.displayInfos.map({
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
          |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, $ordering, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                countryCodes.map(f => Json.arr(f: _*)).orNull,
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
                countryCodes.map(f => Json.arr(f: _*)).orNull,
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
          (t, _) <- t.query(statement, Json.arr(binds: _*))
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

  private[this] def mapRowToDependentColumnInformation(row: JsonArray): DependentColumnInformation = {

    val tableId = row.get[TableId](0)
    val columnId = row.get[ColumnId](1)
    val kind = TableauxDbType(row.get[String](2))
    val identifier = row.get[Boolean](3)
    val groupColumnIds = Option(row.get[String](4))
      .map(str => Json.fromArrayString(str).asScala.map(_.asInstanceOf[Int].toLong).toSeq)
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

  def retrieve(table: Table, columnId: ColumnId)(implicit user: TableauxUser): Future[ColumnType[_]] = {
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
       |  decimal_digits
       |""".stripMargin

  private def retrieveOne(table: Table, columnId: ColumnId, depth: Int)(
      implicit user: TableauxUser
  ): Future[ColumnType[_]] = {
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
          retrieveAll(table)
            .map(_.find(_.id == g.id).get)

        case column =>
          Future.successful(column)
      })
    } yield mappedColumn
  }

  def retrieveAll(table: Table)(implicit user: TableauxUser): Future[Seq[ColumnType[_]]] =
    retrieveColumns(table, MAX_DEPTH, identifiersOnly = false)

  private def retrieveColumns(table: Table, depth: Int, identifiersOnly: Boolean)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[_]]] = {
    for {
      result <- connection.query(generateRetrieveColumnsQuery(identifiersOnly), Json.arr(table.id))

      mappedColumns <- {
        val futures = resultObjectToJsonArray(result)
          .map(mapRowResultToColumnType(table, _, depth))

        Future.sequence(futures)
      }
    } yield {
      val columns = mappedColumns
        .map({
          case g: GroupColumn =>
            // fill GroupColumn with life!
            // ... till now GroupColumn only was a placeholder
            val groupedColumns = mappedColumns.filter(_.columnInformation.groupColumnIds.contains(g.id))
            GroupColumn(g.columnInformation, groupedColumns, g.formatPattern, g.showMemberColumns)

          case c => c
        })

      prependConcatColumnIfNecessary(table, columns)
    }
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
  ): Future[Seq[ColumnType[_]]] = {
    for {
      // we need to retrieve identifiers only...
      // ... otherwise we will end up in a infinite loop
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

  private def prependConcatColumnIfNecessary(table: Table, columns: Seq[ColumnType[_]])(
      implicit user: TableauxUser
  ): Seq[ColumnType[_]] = {
    val identifierColumns = columns.filter(_.identifier)

    val formatPattern =
      if (!isColumnGroupMatchingToFormatPattern(table.concatFormatPattern, identifierColumns)) {
        val columnsIds = identifierColumns.map(_.id).mkString(", ");
        val formatPatternString = table.concatFormatPattern.map(_.toString).orNull;

        logger.warn(s"IdentifierColumns ($columnsIds) don't match to formatPattern '$formatPatternString'")

        None
      } else {
        table.concatFormatPattern
      }

    identifierColumns.size match {
      case x if x >= 2 => {
        // in case of two or more identifier columns we preserve the order of column
        // and a concatcolumn in front of all columns
        columns.+:(ConcatColumn(ConcatColumnInformation(table), identifierColumns, formatPattern))
      }
      case x if x == 1 =>
        // in case of one identifier column we don't get a concat column
        // but the identifier column will be the first
        columns.sortBy(_.identifier)(Ordering[Boolean].reverse)
      case _ =>
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
  )(implicit user: TableauxUser): Future[ColumnType[_]] = {
    kind match {
      case AttachmentType =>
        Future(AttachmentColumn(columnInformation))

      case StatusType =>
        mapStatusColumn(columnInformation, rules)

      case LinkType =>
        mapLinkColumn(depth, columnInformation)

      case GroupType =>
        // placeholder for now, grouped columns will be filled in later
        Future(GroupColumn(columnInformation, Seq.empty, formatPattern, showMemberColumns))

      case _ =>
        Future(SimpleValueColumn(kind, languageType, columnInformation))
    }
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
  ): Future[Seq[ColumnType[_]]] = {

    var valueTypeMap: Map[ColumnId, Seq[_]] = Map()

    def calcDependentColumnIdsFromValuesWithSideEffect(values: JsonArray): Seq[ColumnId] = {

      asSeqOf[JsonObject](values)
        .map(value => {
          if (value.containsKey("values")) {
            val newValues = value.getJsonArray("values")
            calcDependentColumnIdsFromValuesWithSideEffect(newValues)
          } else {
            val columnId = value.getLong("column").asInstanceOf[ColumnId]
            valueTypeMap += (columnId -> (Seq(value.getValue("value")) ++ valueTypeMap.getOrElse(columnId, Seq())))
            Seq(columnId)
          }
        })
        .flatten
    }

    val dependentColumnIds = asSeqOf[JsonObject](rules)
      .map(json => {
        val values = json.getJsonObject("conditions").getJsonArray("values")
        calcDependentColumnIdsFromValuesWithSideEffect(values)
      })
      .flatten
      .distinct

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
          case TextType => (valueToCompare => valueToCompare.isInstanceOf[String], "String")
          case ShortTextType => (valueToCompare => valueToCompare.isInstanceOf[String], "String")
          case RichTextType => (valueToCompare => valueToCompare.isInstanceOf[String], "String")
          case NumericType => (valueToCompare => valueToCompare.isInstanceOf[Number], "Number")
          case BooleanType => (valueToCompare => valueToCompare.isInstanceOf[Boolean], "Boolean")
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

  private def mapLinkColumn(depth: Int, columnInformation: ColumnInformation)(
      implicit user: TableauxUser
  ): Future[LinkColumn] = {
    for {
      (linkId, linkDirection, toTable) <- retrieveLinkInformation(columnInformation.table, columnInformation.id)

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
      LinkColumn(columnInformation, toColumn, linkId, linkDirection)
    }
  }

  private def mapRowResultToColumnType(table: Table, row: JsonArray, depth: Int)(
      implicit user: TableauxUser
  ): Future[ColumnType[_]] = {
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
          .map(str => Json.fromArrayString(str).asScala.map({ case code: String => code }).toSeq)
          .getOrElse(Seq.empty[String])

        MultiCountry(CountryCodes(codes))
    }

    val groupColumnIds = Option(row.get[String](10))
      .map(str => Json.fromArrayString(str).asScala.map(_.asInstanceOf[Int].toLong).toSeq)
      .getOrElse(Seq.empty[ColumnId])

    val formatPattern = Option(row.get[String](11))
    val hidden = row.get[Boolean](12)
    val maxLength = Option(row.get[Int](13))
    val minLength = Option(row.get[Int](14))
    val showMemberColumns = row.get[Boolean](15)
    val decimalDigits = Option(row.get[Int](16))

    for {
      displayInfoSeq <- retrieveDisplayInfo(table, columnId)

      columnInformation = BasicColumnInformation(
        table,
        columnId,
        columnName,
        ordering,
        identifier,
        displayInfoSeq,
        groupColumnIds,
        separator,
        attributes,
        hidden,
        maxLength,
        minLength,
        decimalDigits
      )

      column <- mapColumn(depth, kind, languageType, columnInformation, formatPattern, rules, showMemberColumns)
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
  ): Future[(LinkId, LinkDirection, Table)] = {
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
          | final_cascade
          |FROM system_link_table
          |WHERE link_id = (
          |  SELECT link_id
          |  FROM system_columns
          |  WHERE table_id = ? AND column_id = ?
          |)""".stripMargin,
        Json.arr(fromTable.id, columnId)
      )

      (linkId, linkDirection) = {
        val res = selectNotNull(result).head

        val table1 = res.getLong(0).longValue()
        val table2 = res.getLong(1).longValue()
        val linkId = res.getLong(2).longValue()
        val cardinality1 = res.getLong(3).intValue()
        val cardinality2 = res.getLong(4).intValue()
        val deleteCascade = res.getBoolean(5)
        val archiveCascade = res.getBoolean(6)
        val finalCascade = res.getBoolean(7)

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
          )
        )
      }

      toTable <- tableStruc.retrieve(linkDirection.to, isInternalCall = true)

    } yield (linkId, linkDirection, toTable)
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
          case c: ColumnType[_] => deleteSimpleColumn(c)
        }
      }
    } yield ()
  }

  private def checkForStatusColumnDependency(
      columnId: ColumnId,
      columns: Seq[ColumnType[_]],
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

  private def deleteSimpleColumn(column: ColumnType[_]): Future[Unit] = {
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
      columnName: String
  ): String = s"UPDATE system_columns SET $columnName = ? WHERE table_id = ? AND column_id = ?"

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
      decimalDigits: Option[Int]
  )(implicit user: TableauxUser): Future[ColumnType[_]] = {
    val tableId = table.id

    def maybeUpdateColumn[VALUE_TYPE](
        t: DbTransaction,
        columnName: String,
        value: Option[VALUE_TYPE],
        trans: VALUE_TYPE => _ = (v: VALUE_TYPE) => v
    ): Future[(DbTransaction, JsonObject)] = {
      optionToValidFuture(
        value,
        t,
        { v: VALUE_TYPE => t.query(getUpdateQueryFor(columnName), Json.arr(trans(v), tableId, columnId)) }
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
      (t, resultAttributes) <- maybeUpdateColumn(t, "attributes", attributes, (a: JsonObject) => a.encode())
      (t, resultRules) <- maybeUpdateColumn(t, "rules", rules, (r: JsonArray) => r.encode())
      (t, resultCountryCodes) <-
        maybeUpdateColumn(t, "country_codes", countryCodes, (c: Seq[String]) => Json.arr(c: _*))
      (t, resultHidden) <- maybeUpdateColumn(t, "hidden", hidden)
      (t, resultShowMemberColumns) <- maybeUpdateColumn(t, "show_member_columns", showMemberColumns)
      (t, resultDecimalDigits) <- maybeUpdateColumn(t, "decimal_digits", decimalDigits)

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
        { k: TableauxDbType =>
          t.query(
            s"ALTER TABLE user_table_$tableId ALTER COLUMN column_$columnId TYPE ${k.toDbType} USING column_$columnId::${k.toDbType}"
          )

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
          resultDecimalDigits
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
              (t, _) <- t.query(statement, Json.arr(binds: _*))
            } yield t
        }
      case None => Future.successful(t)
    }
  }
}
