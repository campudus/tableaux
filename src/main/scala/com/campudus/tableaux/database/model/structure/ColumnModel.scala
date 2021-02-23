package com.campudus.tableaux.database.model.structure

import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import com.campudus.tableaux._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.structure.CachedColumnModel._
import com.campudus.tableaux.database.model.structure.ColumnModel.isColumnGroupMatchingToFormatPattern
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.google.common.cache.CacheBuilder
import com.typesafe.scalalogging.LazyLogging
import org.vertx.scala.core.json._
import scalacache._
import scalacache.guava._
import scalacache.modes.scalaFuture._

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedSet
import scala.concurrent.Future

object CachedColumnModel {

  /**
    * Default never expire
    */
  val DEFAULT_EXPIRE_AFTER_ACCESS: Long = -1L

  /**
    * Max. 10k cached values per column
    */
  val DEFAULT_MAXIMUM_SIZE: Long = 10000L
}

class CachedColumnModel(
    val config: JsonObject,
    override val connection: DatabaseConnection
)(
    implicit requestContext: RequestContext,
    roleModel: RoleModel
) extends ColumnModel(connection) {

  implicit val scalaCache: Cache[Object] = GuavaCache(createCache())

  private def createCache() = {
    val builder = CacheBuilder
      .newBuilder()

    val expireAfterAccess = config.getLong("expireAfterAccess", DEFAULT_EXPIRE_AFTER_ACCESS).longValue()
    if (expireAfterAccess > 0) {
      builder.expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
    } else {
      logger.info("Cache will not expire!")
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

  override def retrieve(table: Table, columnId: ColumnId): Future[ColumnType[_]] = {
    cachingF[Future, Object]("retrieve", table.id, columnId)(None)(
      super.retrieve(table, columnId)
    ).asInstanceOf[Future[ColumnType[_]]]
  }

  override def retrieveAll(table: Table): Future[Seq[ColumnType[_]]] = {
    cachingF[Future, Object]("retrieveAll", table.id)(None)(
      super.retrieveAll(table)
    ).asInstanceOf[Future[Seq[ColumnType[_]]]]
  }

  override def createColumns(table: Table, createColumns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = {
    for {
      r <- super.createColumns(table, createColumns)
      _ <- removeCache(table.id, None)
    } yield r
  }

  override def createColumn(table: Table, createColumn: CreateColumn): Future[ColumnType[_]] = {
    for {
      r <- super.createColumn(table, createColumn)
      _ <- removeCache(table.id, None)
    } yield r
  }

  override def delete(table: Table,
                      columnId: ColumnId,
                      bothDirections: Boolean,
                      checkForLastColumn: Boolean = true): Future[Unit] = {
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
      separator: Option[Boolean]
  ): Future[ColumnType[_]] = {
    for {
      _ <- removeCache(table.id, Some(columnId))
      r <- super
        .change(table, columnId, columnName, ordering, kind, identifier, displayInfos, countryCodes, separator)
    } yield r
  }
}

object ColumnModel extends LazyLogging {

  def isColumnGroupMatchingToFormatPattern(formatPattern: Option[String],
                                           groupedColumns: Seq[ColumnType[_]]): Boolean = {
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
            s"with columnIDs (${columnIDs.mkString(", ")})")

        distinctWildcards == columnIDs
      }
      case None => true
    }
  }
}

class ColumnModel(val connection: DatabaseConnection)(
    implicit requestContext: RequestContext,
    roleModel: RoleModel
) extends DatabaseQuery {

  private lazy val tableStruc = new TableModel(connection)

  private val MAX_DEPTH = 5

  def createColumns(table: Table, createColumns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = {
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

  def createColumn(table: Table, createColumn: CreateColumn): Future[ColumnType[_]] = {

    def applyColumnInformation(id: ColumnId, ordering: Ordering, displayInfos: Seq[DisplayInfo]) =
      BasicColumnInformation(table, id, ordering, displayInfos, createColumn)

    createColumn match {
      case simpleColumnInfo: CreateSimpleColumn =>
        createValueColumn(table.id, simpleColumnInfo)
          .map({
            case CreatedColumnInformation(_, id, ordering, displayInfos) =>
              SimpleValueColumn(simpleColumnInfo.kind,
                                simpleColumnInfo.languageType,
                                applyColumnInformation(id, ordering, displayInfos))
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

      case groupColumnInfo: CreateGroupColumn =>
        createGroupColumn(table, groupColumnInfo)
          .map({
            case CreatedColumnInformation(_, id, ordering, displayInfos) =>
              // For simplification we return GroupColumn without grouped columns...
              // ... StructureController will retrieve these anyway
              GroupColumn(applyColumnInformation(id, ordering, displayInfos), Seq.empty, groupColumnInfo.formatPattern)
          })
    }
  }

  private def createGroupColumn(
      table: Table,
      groupColumnInfo: CreateGroupColumn
  ): Future[CreatedColumnInformation] = {
    val tableId = table.id

    connection.transactional { t =>
      for {
        // retrieve all to-be-grouped columns
        groupedColumns <- retrieveAll(table)
          .map(_.filter(column => groupColumnInfo.groups.contains(column.id)))

        // do some validation before creating GroupColumn
        _ = {
          if (groupedColumns.size != groupColumnInfo.groups.size) {
            throw UnprocessableEntityException(
              s"GroupColumn (${groupColumnInfo.name}) couldn't be created because some columns don't exist")
          }

          if (groupedColumns.exists(_.kind == GroupType)) {
            throw UnprocessableEntityException(
              s"GroupColumn (${groupColumnInfo.name}) can't contain another GroupColumn")
          }

          if (!isColumnGroupMatchingToFormatPattern(groupColumnInfo.formatPattern, groupedColumns)) {
            throw UnprocessableEntityException(
              s"GroupColumns (${groupedColumns.map(_.id).mkString(", ")}) don't match to formatPattern " +
                s"${"\"" + groupColumnInfo.formatPattern.map(_.toString).orNull + "\""}")
          }
        }

        (t, columnInfo) <- insertSystemColumn(t, tableId, groupColumnInfo, None, groupColumnInfo.formatPattern)

        // insert group information
        insertPlaceholder = groupColumnInfo.groups.map(_ => "(?, ?, ?)").mkString(", ")
        (t, _) <- t.query(
          s"INSERT INTO system_column_groups(table_id, group_column_id, grouped_column_id) VALUES $insertPlaceholder",
          Json.arr(groupColumnInfo.groups.flatMap(Seq(tableId, columnInfo.columnId, _)): _*)
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
        (t, columnInfo) <- insertSystemColumn(t, tableId, simpleColumnInfo, None, None)
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
        (t, columnInfo) <- insertSystemColumn(t, tableId, attachmentColumnInfo, None, None)
      } yield (t, columnInfo)
    }
  }

  private def createLinkColumn(
      table: Table,
      linkColumnInfo: CreateLinkColumn
  ): Future[(LinkId, ColumnType[_], CreatedColumnInformation)] = {
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
             |  delete_cascade
             |) VALUES (?, ?, ?, ?, ?) RETURNING link_id""".stripMargin,
          Json.arr(
            tableId,
            linkColumnInfo.toTable,
            linkColumnInfo.constraint.cardinality.from,
            linkColumnInfo.constraint.cardinality.to,
            linkColumnInfo.constraint.deleteCascade
          )
        )
        linkId = insertNotNull(result).head.get[Long](0)

        // insert link column on source table
        (t, columnInfo) <- insertSystemColumn(t, tableId, linkColumnInfo, Some(linkId), None)

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
            insertSystemColumn(t, linkColumnInfo.toTable, copiedLinkColumnInfo, Some(linkId), None)
              .map({
                case (t, _) => t
              })
          } else {
            Future(t)
          }
        }

        (t, _) <- t.query(s"""|CREATE TABLE link_table_$linkId (
                              | id_1 bigint,
                              | id_2 bigint,
                              | ordering_1 serial,
                              | ordering_2 serial,
                              |
                              | PRIMARY KEY(id_1, id_2),
                              |
                              | CONSTRAINT link_table_${linkId}_foreign_1
                              | FOREIGN KEY(id_1) REFERENCES user_table_$tableId (id) ON DELETE CASCADE,
                              | CONSTRAINT link_table_${linkId}_foreign_2
                              | FOREIGN KEY(id_2) REFERENCES user_table_${linkColumnInfo.toTable} (id) ON DELETE CASCADE
                              |)""".stripMargin)
      } yield {
        (t, (linkId, toCol, columnInfo))
      }
    }
  }

  private def insertSystemColumn(
      t: connection.Transaction,
      tableId: TableId,
      createColumn: CreateColumn,
      linkId: Option[LinkId],
      formatPattern: Option[String]
  ): Future[(connection.Transaction, CreatedColumnInformation)] = {

    def insertStatement(tableId: TableId, ordering: String) = {
      s"""|INSERT INTO system_columns (
          | table_id,
          | column_id,
          | column_type,
          | user_column_name,
          | ordering,
          | link_id,
          | multilanguage,
          | identifier,
          | format_pattern,
          | country_codes,
          | separator
          | )
          | VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, $ordering, ?, ?, ?, ?, ?, ?)
          | RETURNING column_id, ordering
          |""".stripMargin
    }

    val countryCodes = createColumn.languageType match {
      case MultiCountry(codes) => Some(codes.codes)
      case _ => None
    }

    def insertColumn(t: connection.Transaction): Future[(connection.Transaction, CreatedColumnInformation)] = {
      for {
        t <- t
          .selectSingleValue[Long]("SELECT COUNT(*) FROM system_columns WHERE table_id = ? AND user_column_name = ?",
                                   Json.arr(tableId, createColumn.name))
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
                createColumn.separator
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
                createColumn.separator
              )
            )
        }
      } yield {
        val resultRow = insertNotNull(result).head
        (t, CreatedColumnInformation(tableId, resultRow.getLong(0), resultRow.getLong(1)))
      }
    }

    def insertColumnLang(
        t: connection.Transaction,
        displayInfos: ColumnDisplayInfos
    ): Future[(connection.Transaction, Seq[DisplayInfo])] = {
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
         | FROM system_columns d JOIN system_column_groups g ON (d.table_id = g.table_id AND d.column_id = g.group_column_id)
         | WHERE d.table_id = ? AND g.grouped_column_id = ?""".stripMargin

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
         | FROM system_link_table l JOIN system_columns d ON (l.link_id = d.link_id) LEFT JOIN system_column_groups g ON (d.table_id = g.table_id AND d.column_id = g.grouped_column_id)
         | WHERE (l.table_id_1 = ? OR l.table_id_2 = ?) AND d.table_id != ?
         | GROUP BY d.table_id, d.column_id
         | ORDER BY d.table_id, d.column_id""".stripMargin

    for {
      dependentColumns <- connection.query(select, Json.arr(tableId, tableId, tableId))

      dependentColumnInformation = resultObjectToJsonArray(dependentColumns)
        .map(mapRowToDependentColumnInformation)

      recursiveDependentColumnInformation <- dependentColumnInformation.foldLeft(
        Future.successful(dependentColumnInformation))({
        case (dependentColumnInformationFuture, dependentColumn) =>
          for {
            dependentColumnInformation <- dependentColumnInformationFuture

            resultSeq <- if (dependentColumn.identifier) {
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
            val bidirectional = row.get[Boolean](6)

            val result = (
              linkId,
              LinkDirection(
                tableId,
                tableId1,
                tableId2,
                cardinality1,
                cardinality2,
                deleteCascade
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

  def retrieve(table: Table, columnId: ColumnId): Future[ColumnType[_]] = {
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

  private def retrieveOne(table: Table, columnId: ColumnId, depth: Int): Future[ColumnType[_]] = {
    val select =
      s"""
         |SELECT
         |  column_id,
         |  user_column_name,
         |  column_type,
         |  ordering,
         |  multilanguage,
         |  identifier,
         |  separator,
         |  array_to_json(country_codes),
         |  (
         |    SELECT json_agg(group_column_id) FROM system_column_groups
         |    WHERE table_id = c.table_id AND grouped_column_id = c.column_id
         |  ) AS group_column_ids,
         |  format_pattern
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

  def retrieveAll(table: Table): Future[Seq[ColumnType[_]]] =
    retrieveColumns(table, MAX_DEPTH, identifiersOnly = false)

  private def retrieveColumns(table: Table, depth: Int, identifiersOnly: Boolean): Future[Seq[ColumnType[_]]] = {
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
            GroupColumn(g.columnInformation, groupedColumns, g.formatPattern)

          case c => c
        })

      prependConcatColumnIfNecessary(table, columns)
    }
  }

  private def generateRetrieveColumnsQuery(identifiersOnly: Boolean): String = {
    val identifierFilter = if (identifiersOnly) {
      // select either identifier column and/or
      // ... grouped columns if GroupColumn is an identifier
      """
        |AND identifier = TRUE OR
        |(
        | SELECT COUNT(*)
        | FROM
        | system_columns sc
        | LEFT JOIN system_column_groups g
        |   ON (sc.table_id = g.table_id AND sc.column_id = g.grouped_column_id)
        | LEFT JOIN system_columns sc2
        |   ON (sc2.table_id = g.table_id AND sc2.column_id = g.group_column_id)
        | WHERE sc2.identifier = TRUE AND sc.column_id = c.column_id AND sc.table_id = c.table_id
        |) > 0""".stripMargin
    } else {
      ""
    }

    s"""
       |SELECT
       |  column_id,
       |  user_column_name,
       |  column_type,
       |  ordering,
       |  multilanguage,
       |  identifier,
       |  separator,
       |  array_to_json(country_codes),
       |  (
       |    SELECT json_agg(group_column_id) FROM system_column_groups
       |    WHERE table_id = c.table_id AND grouped_column_id = c.column_id
       |  ) AS group_column_ids,
       |  format_pattern
       |FROM system_columns c
       |WHERE
       |  table_id = ?
       |  $identifierFilter
       |ORDER BY ordering, column_id""".stripMargin
  }

  private def retrieveIdentifiers(table: Table, depth: Int): Future[Seq[ColumnType[_]]] = {
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

  private def prependConcatColumnIfNecessary(table: Table, columns: Seq[ColumnType[_]]): Seq[ColumnType[_]] = {
    val identifierColumns = columns.filter(_.identifier)

    identifierColumns.size match {
      case x if x >= 2 =>
        // in case of two or more identifier columns we preserve the order of column
        // and a concatcolumn in front of all columns
        columns.+:(ConcatColumn(ConcatColumnInformation(table), identifierColumns))
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
      formatPattern: Option[String]
  ): Future[ColumnType[_]] = {
    kind match {
      case AttachmentType =>
        Future(AttachmentColumn(columnInformation))

      case LinkType =>
        mapLinkColumn(depth, columnInformation)

      case GroupType =>
        // placeholder for now, grouped columns will be filled in later
        Future(GroupColumn(columnInformation, Seq.empty, formatPattern))

      case _ =>
        Future(SimpleValueColumn(kind, languageType, columnInformation))
    }
  }

  private def mapLinkColumn(depth: Int, columnInformation: ColumnInformation): Future[LinkColumn] = {
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

  private def mapRowResultToColumnType(table: Table, row: JsonArray, depth: Int): Future[ColumnType[_]] = {
    val columnId = row.get[ColumnId](0)
    val columnName = row.get[String](1)
    val kind = TableauxDbType(row.get[String](2))
    val ordering = row.get[Ordering](3)
    val identifier = row.get[Boolean](5)
    val separator = row.get[Boolean](6)

    val languageType = LanguageType(Option(row.get[String](4))) match {
      case LanguageNeutral => LanguageNeutral
      case MultiLanguage => MultiLanguage
      case c: MultiCountry =>
        val codes = Option(row.get[String](7))
          .map(str => Json.fromArrayString(str).asScala.map({ case code: String => code }).toSeq)
          .getOrElse(Seq.empty[String])

        MultiCountry(CountryCodes(codes))
    }

    val groupColumnIds = Option(row.get[String](8))
      .map(str => Json.fromArrayString(str).asScala.map(_.asInstanceOf[Int].toLong).toSeq)
      .getOrElse(Seq.empty[ColumnId])

    val formatPattern = Option(row.get[String](9))

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
        separator
      )

      column <- mapColumn(depth, kind, languageType, columnInformation, formatPattern)
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

  private def retrieveLinkInformation(fromTable: Table, columnId: ColumnId): Future[(LinkId, LinkDirection, Table)] = {
    for {
      result <- connection.query(
        """
          |SELECT
          | table_id_1,
          | table_id_2,
          | link_id,
          | cardinality_1,
          | cardinality_2,
          | delete_cascade
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

        (
          linkId,
          LinkDirection(
            fromTable.id,
            table1,
            table2,
            cardinality1,
            cardinality2,
            deleteCascade
          )
        )
      }

      toTable <- tableStruc.retrieve(linkDirection.to, isInternalCall = true)

    } yield (linkId, linkDirection, toTable)
  }

  def deleteLinkBothDirections(table: Table, columnId: ColumnId): Future[Unit] = {
    delete(table, columnId, bothDirections = true, checkForLastColumn = false)
  }

  def delete(table: Table, columnId: ColumnId): Future[Unit] =
    delete(table, columnId, bothDirections = false, checkForLastColumn = true)

  protected def delete(table: Table,
                       columnId: ColumnId,
                       bothDirections: Boolean,
                       checkForLastColumn: Boolean = true): Future[Unit] = {

    // Retrieve all filter for columnId and check if columns is not empty
    // If columns is empty last column would be deleted => error
    for {
      columns <- retrieveAll(table)
        .filter(_.nonEmpty)
        .recoverWith({
          case _: NoSuchElementException =>
            Future.failed(NotFoundInDatabaseException("No column found at all", "no-column-found"))
        })

      _ <- if (checkForLastColumn) {
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
          throw NotFoundInDatabaseException("Column can't be deleted because it doesn't exist.", "delete-non-existing"))

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

  private def deleteLink(column: LinkColumn, bothDirections: Boolean): Future[Unit] = {
    val tableId = column.table.id
    val columnId = column.id

    for {
      t <- connection.begin()

      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE column_id = ? AND table_id = ?",
                             Json.arr(column.id, column.table.id))
      linkId = selectCheckSize(result, 1).head.get[Long](0)

      (t, result) <- t.query("SELECT COUNT(*) = 1 FROM system_columns WHERE link_id = ?", Json.arr(linkId))
      unidirectional = selectCheckSize(result, 1).head.getBoolean(0).booleanValue()

      (t, _) <- {
        // We only want to delete both directions
        // when we delete one of the two tables
        // which are linked together.
        // ColumnModel.deleteLink() with both = true
        // is called by StructureController.deleteTable().
        val deleteFuture = if (bothDirections) {
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

  private def deleteSystemColumn(t: connection.Transaction,
                                 tableId: TableId,
                                 columnId: ColumnId): Future[connection.Transaction] = {
    for {
      (t, _) <- t
        .query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
        .map({ case (t, json) => (t, deleteNotNull(json)) })
    } yield t
  }

  private def deleteAnnotations(t: connection.Transaction,
                                tableId: TableId,
                                columnId: ColumnId): Future[connection.Transaction] = {
    for {
      (t, _) <- t.query(s"DELETE FROM user_table_annotations_$tableId WHERE column_id = ?", Json.arr(columnId))
    } yield t
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
      separator: Option[Boolean]
  ): Future[ColumnType[_]] = {
    val tableId = table.id

    for {
      t <- connection.begin()

      // change column settings
      (t, resultName) <- optionToValidFuture(
        columnName,
        t, { name: String =>
          {
            t.query(s"UPDATE system_columns SET user_column_name = ? WHERE table_id = ? AND column_id = ?",
                    Json.arr(name, tableId, columnId))
          }
        }
      )
      (t, resultOrdering) <- optionToValidFuture(ordering, t, { ord: Ordering =>
        {
          t.query(s"UPDATE system_columns SET ordering = ? WHERE table_id = ? AND column_id = ?",
                  Json.arr(ord, tableId, columnId))
        }
      })
      (t, resultKind) <- optionToValidFuture(
        kind,
        t, { k: TableauxDbType =>
          {
            t.query(s"UPDATE system_columns SET column_type = ? WHERE table_id = ? AND column_id = ?",
                    Json.arr(k.name, tableId, columnId))
          }
        }
      )
      (t, resultIdentifier) <- optionToValidFuture(
        identifier,
        t, { ident: Boolean =>
          {
            t.query(s"UPDATE system_columns SET identifier = ? WHERE table_id = ? AND column_id = ?",
                    Json.arr(ident, tableId, columnId))
          }
        }
      )
      (t, resultSeparator) <- optionToValidFuture(
        separator,
        t, { sep: Boolean =>
          {
            t.query(s"UPDATE system_columns SET separator = ? WHERE table_id = ? AND column_id = ?",
                    Json.arr(sep, tableId, columnId))
          }
        }
      )
      (t, resultCountryCodes) <- optionToValidFuture(
        countryCodes,
        t, { codes: Seq[String] =>
          {
            t.query(s"UPDATE system_columns SET country_codes = ? WHERE table_id = ? AND column_id = ?",
                    Json.arr(Json.arr(codes: _*), tableId, columnId))
          }
        }
      )

      // change display information
      t <- insertOrUpdateColumnLangInfo(t, table.id, columnId, displayInfos)

      // change column kind
      (t, _) <- optionToValidFuture(
        kind,
        t, { k: TableauxDbType =>
          {
            t.query(
              s"ALTER TABLE user_table_$tableId ALTER COLUMN column_$columnId TYPE ${k.toDbType} USING column_$columnId::${k.toDbType}")
          }
        }
      ).recoverWith(t.rollbackAndFail())

      _ <- Future(checkUpdateResults(resultName, resultOrdering, resultKind, resultIdentifier, resultCountryCodes, resultSeparator))
        .recoverWith(t.rollbackAndFail())

      _ <- t.commit()

      column <- retrieve(table, columnId)
    } yield column
  }

  private def insertOrUpdateColumnLangInfo(
      t: connection.Transaction,
      tableId: TableId,
      columnId: ColumnId,
      optDisplayInfos: Option[Seq[DisplayInfo]]
  ): Future[connection.Transaction] = {

    optDisplayInfos match {
      case Some(displayInfos) =>
        val dis = ColumnDisplayInfos(tableId, columnId, displayInfos)
        dis.entries.foldLeft(Future.successful(t)) {
          case (future, di) =>
            for {
              t <- future
              (t, select) <- t.query(
                "SELECT COUNT(*) FROM system_columns_lang WHERE table_id = ? AND column_id = ? AND langtag = ?",
                Json.arr(tableId, columnId, di.langtag))
              count = select.getJsonArray("results").getJsonArray(0).getLong(0)
              (statement, binds) = if (count > 0) {
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
