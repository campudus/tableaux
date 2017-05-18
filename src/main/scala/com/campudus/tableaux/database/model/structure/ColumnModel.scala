package com.campudus.tableaux.database.model.structure

import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.{DatabaseException, NotFoundInDatabaseException, ShouldBeUniqueException}
import com.google.common.cache.CacheBuilder
import com.google.common.cache.{Cache => GuavaBuiltCache}
import org.vertx.scala.core.json._

import scala.concurrent.Future

object CachedColumnModel {

  /**
    * Default never expire
    */
  val DEFAULT_EXPIRE_AFTER_ACCESS: Long = -1l

  /**
    * Max. 10k cached values per column
    */
  val DEFAULT_MAXIMUM_SIZE: Long = 10000l
}

class CachedColumnModel(val config: JsonObject, override val connection: DatabaseConnection)
    extends ColumnModel(connection) {

  import CachedColumnModel._
  import scalacache.caching
  import scalacache.ScalaCache
  import scalacache.guava.GuavaCache

  implicit val scalaCache = ScalaCache(GuavaCache(createCache()))

  private def createCache(): GuavaBuiltCache[String, Object] = {
    val builder = CacheBuilder
      .newBuilder()

    val expireAfterAccess = config.getLong("expireAfterAccess", DEFAULT_EXPIRE_AFTER_ACCESS).toLong
    if (expireAfterAccess > 0) {
      builder.expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
    } else {
      logger.info("Cache will not expire!")
    }

    val maximumSize = config.getLong("maximumSize", DEFAULT_MAXIMUM_SIZE).toLong
    if (maximumSize > 0) {
      builder.maximumSize(maximumSize)
    }

    builder.recordStats()

    builder.build[String, Object]
  }

  private def removeCache(tableId: TableId, columnId: Option[ColumnId]): Future[Unit] = {
    import scalacache.remove

    for {
      _ <- columnId match {
        case Some(id) => remove("retrieve", tableId, id)
        case None => Future.successful(())
      }
      _ <- remove("retrieveAll", tableId)

      dependencies <- retrieveDependencies(tableId)

      _ <- Future.sequence(dependencies.map({
        case (dependentTableId, dependentColumnId) =>
          for {
            _ <- remove("retrieve", dependentTableId, dependentColumnId)
            _ <- remove("retrieveAll", dependentTableId)
          } yield ()
      }))
    } yield ()
  }

  override def retrieve(table: Table, columnId: ColumnId): Future[ColumnType[_]] = {
    caching("retrieve", table.id, columnId) {
      super.retrieve(table, columnId)
    }
  }

  override def retrieveAll(table: Table): Future[Seq[ColumnType[_]]] = {
    caching("retrieveAll", table.id) {
      super.retrieveAll(table)
    }
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

  override def delete(table: Table, columnId: ColumnId, bothDirections: Boolean): Future[Unit] = {
    for {
      r <- super.delete(table, columnId, bothDirections)
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
      countryCodes: Option[Seq[String]]
  ): Future[ColumnType[_]] = {
    for {
      _ <- removeCache(table.id, Some(columnId))
      r <- super.change(table, columnId, columnName, ordering, kind, identifier, displayInfos, countryCodes)
    } yield r
  }
}

class ColumnModel(val connection: DatabaseConnection) extends DatabaseQuery {

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
    createColumn match {
      case simpleColumnInfo: CreateSimpleColumn =>
        createValueColumn(table.id, simpleColumnInfo).map({
          case CreatedColumnInformation(tableId, id, ordering, displayInfos) =>
            SimpleValueColumn(
              simpleColumnInfo.kind,
              simpleColumnInfo.languageType,
              BasicColumnInformation(table,
                                     id,
                                     simpleColumnInfo.name,
                                     ordering,
                                     simpleColumnInfo.identifier,
                                     displayInfos)
            )
        })

      case linkColumnInfo: CreateLinkColumn =>
        for {
          toTable <- tableStruc.retrieve(linkColumnInfo.toTable)
          toTableColumns <- retrieveAll(toTable).flatMap({ columns =>
            {
              if (columns.isEmpty) {
                Future.failed(
                  NotFoundInDatabaseException(s"Link points at table ${toTable.id} without columns", "no-columns"))
              } else {
                Future.successful(columns)
              }
            }
          })

          toCol = toTableColumns.head

          (linkId, CreatedColumnInformation(_, id, ordering, displayInfos)) <- createLinkColumn(table,
                                                                                                toTable,
                                                                                                linkColumnInfo)
        } yield
          LinkColumn(
            BasicColumnInformation(table, id, linkColumnInfo.name, ordering, linkColumnInfo.identifier, displayInfos),
            toCol,
            linkId,
            LeftToRight(table.id, linkColumnInfo.toTable, linkColumnInfo.constraint)
          )

      case attachmentColumnInfo: CreateAttachmentColumn =>
        createAttachmentColumn(table.id, attachmentColumnInfo).map({
          case CreatedColumnInformation(tableId, id, ordering, displayInfos) =>
            AttachmentColumn(
              BasicColumnInformation(table,
                                     id,
                                     attachmentColumnInfo.name,
                                     ordering,
                                     attachmentColumnInfo.identifier,
                                     displayInfos))
        })
    }
  }

  private def createValueColumn(
      tableId: TableId,
      simpleColumnInfo: CreateSimpleColumn
  ): Future[CreatedColumnInformation] = {
    connection.transactional { t =>
      for {
        (t, columnInfo) <- insertSystemColumn(t, tableId, simpleColumnInfo, None)
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
        (t, columnInfo) <- insertSystemColumn(t, tableId, attachmentColumnInfo, None)
      } yield (t, columnInfo)
    }
  }

  private def createLinkColumn(
      table: Table,
      toTable: Table,
      linkColumnInfo: CreateLinkColumn
  ): Future[(LinkId, CreatedColumnInformation)] = {
    val tableId = table.id

    connection.transactional { t =>
      {
        for {
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
          (t, columnInfo) <- insertSystemColumn(t, tableId, linkColumnInfo, Some(linkId))

          // only add the second link column if tableId != toTableId or singleDirection is false
          t <- {
            if (!linkColumnInfo.singleDirection && tableId != linkColumnInfo.toTable) {
              val copiedLinkColumnInfo = linkColumnInfo.copy(
                name = linkColumnInfo.toName.getOrElse(table.name),
                identifier = false,
                displayInfos = linkColumnInfo.toDisplayInfos.getOrElse({
                  table.displayInfos.map({
                    case DisplayInfo(langtag, Some(name), optDesc) =>
                      NameOnly(langtag, name)
                  })
                })
              )
              // ColumnInfo will be ignored, so we can lose it
              insertSystemColumn(t, linkColumnInfo.toTable, copiedLinkColumnInfo, Some(linkId))
                .map({
                  case (t, _) => t
                })
            } else {
              Future(t)
            }
          }

          (t, _) <- t.query(s"""
CREATE TABLE link_table_$linkId (
 id_1 bigint,
 id_2 bigint,
 ordering_1 serial,
 ordering_2 serial,

 PRIMARY KEY(id_1, id_2),

 CONSTRAINT link_table_${linkId}_foreign_1
 FOREIGN KEY(id_1) REFERENCES user_table_$tableId (id) ON DELETE CASCADE,

 CONSTRAINT link_table_${linkId}_foreign_2
 FOREIGN KEY(id_2) REFERENCES user_table_${linkColumnInfo.toTable} (id) ON DELETE CASCADE
)""".stripMargin)
        } yield {
          (t, (linkId, columnInfo))
        }
      }
    }
  }

  private def insertSystemColumn(
      t: connection.Transaction,
      tableId: TableId,
      createColumn: CreateColumn,
      linkId: Option[LinkId]
  ): Future[(connection.Transaction, CreatedColumnInformation)] = {

    def insertStatement(tableId: TableId, ordering: String) = {
      s"""INSERT INTO system_columns (
table_id,
column_id,
column_type,
user_column_name,
ordering,
link_id,
multilanguage,
identifier,
country_codes
)
VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, $ordering, ?, ?, ?, ?)
RETURNING column_id, ordering""".stripMargin
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
                countryCodes.map(f => Json.arr(f: _*)).orNull
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
                countryCodes.map(f => Json.arr(f: _*)).orNull
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
          (t, result) <- t.query(statement, Json.arr(binds: _*))
        } yield (t, displayInfos.entries)
      } else {
        Future.successful((t, List()))
      }
    }

    for {
      (t, result) <- insertColumn(t)
      (t, resultLang) <- insertColumnLang(t, ColumnDisplayInfos(tableId, result.columnId, createColumn.displayInfos))
    } yield (t, result.copy(displayInfos = createColumn.displayInfos))
  }

  def retrieveDependencies(tableId: TableId, depth: Int = MAX_DEPTH): Future[Seq[(TableId, ColumnId)]] = {
    val select =
      s"""SELECT table_id, column_id, identifier
         | FROM system_link_table l JOIN system_columns d ON (l.link_id = d.link_id)
         | WHERE (l.table_id_1 = ? OR l.table_id_2 = ?) AND d.table_id != ? ORDER BY ordering, column_id""".stripMargin

    for {
      dependentColumns <- connection.query(select, Json.arr(tableId, tableId, tableId))
      mappedColumns <- {
        val futures = resultObjectToJsonArray(dependentColumns)
          .map({ arr =>
            {
              val tableId = arr.get[TableId](0)
              val columnId = arr.get[ColumnId](1)
              val identifier = arr.get[Boolean](2)

              identifier match {
                case false =>
                  Future.successful(Seq((tableId, columnId)))
                case true =>
                  if (depth > 0) {
                    retrieveDependencies(tableId, depth - 1)
                      .map(_ ++ Seq((tableId, columnId)))
                  } else {
                    Future.failed(DatabaseException("Link is too deep. Check schema.", "link-depth"))
                  }
              }
            }
          })

        Future
          .sequence(futures)
          .map(_.flatten)
      }
    } yield mappedColumns
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
        retrieveAll(table).map(_.head)
      case _ =>
        retrieveOne(table, columnId, MAX_DEPTH)
    }
  }

  private def retrieveOne(table: Table, columnId: ColumnId, depth: Int): Future[ColumnType[_]] = {
    val select =
      "SELECT column_id, user_column_name, column_type, ordering, multilanguage, identifier, array_to_json(country_codes) FROM system_columns WHERE table_id = ? AND column_id = ?"

    for {
      result <- connection.query(select, Json.arr(table.id, columnId))
      row = selectNotNull(result).head

      mappedColumn <- mapRowResultToColumnType(table, row, depth)
    } yield mappedColumn
  }

  def retrieveAll(table: Table): Future[Seq[ColumnType[_]]] = retrieveAll(table, MAX_DEPTH)

  private def retrieveAll(table: Table, depth: Int): Future[Seq[ColumnType[_]]] = {
    for {
      (concatColumns, columns) <- retrieveColumns(table, depth, identifierOnly = false)
    } yield concatColumnAndColumns(table, concatColumns, columns)
  }

  private def retrieveIdentifiers(table: Table, depth: Int): Future[Seq[ColumnType[_]]] = {
    for {
      (concatColumns, columns) <- retrieveColumns(table, depth, identifierOnly = true)
    } yield {
      if (concatColumns.isEmpty) {
        throw DatabaseException("Link can not point to table without identifier(s).", "missing-identifier")
      } else {
        concatColumnAndColumns(table, concatColumns, columns)
      }
    }
  }

  private def concatColumnAndColumns(
      table: Table,
      concatColumns: Seq[ColumnType[_]],
      columns: Seq[ColumnType[_]]
  ): Seq[ColumnType[_]] = {
    concatColumns.size match {
      case x if x >= 2 =>
        // in case of two or more identifier columns we preserve the order of column
        // and a concatcolumn in front of all columns
        columns.+:(ConcatColumn(ConcatColumnInformation(table), concatColumns))
      case x if x == 1 =>
        // in case of one identifier column we don't get a concat column
        // but the identifier column will be the first
        columns.sortBy(_.identifier)(Ordering[Boolean].reverse)
      case _ =>
        // no identifier -> return columns
        columns
    }
  }

  private def retrieveColumns(
      table: Table,
      depth: Int,
      identifierOnly: Boolean
  ): Future[(Seq[ColumnType[_]], Seq[ColumnType[_]])] = {
    val identCondition = if (identifierOnly) " AND identifier IS TRUE " else ""
    val select =
      s"""SELECT column_id, user_column_name, column_type, ordering, multilanguage, identifier, array_to_json(country_codes)
         | FROM system_columns
         | WHERE table_id = ? $identCondition ORDER BY ordering, column_id""".stripMargin

    for {
      result <- connection.query(select, Json.arr(table.id))
      mappedColumns <- {
        val futures = resultObjectToJsonArray(result).map(mapRowResultToColumnType(table, _, depth))
        Future.sequence(futures)
      }
    } yield (mappedColumns.filter(_.identifier), mappedColumns)
  }

  private def mapColumn(
      depth: Int,
      kind: TableauxDbType,
      languageType: LanguageType,
      columnInformation: ColumnInformation
  ): Future[ColumnType[_]] = {
    kind match {
      case AttachmentType => mapAttachmentColumn(columnInformation)
      case LinkType => mapLinkColumn(depth, columnInformation)

      case _ => mapValueColumn(kind, languageType, columnInformation)
    }
  }

  private def mapValueColumn(
      kind: TableauxDbType,
      languageType: LanguageType,
      columnInformation: ColumnInformation
  ): Future[SimpleValueColumn[_]] = {
    Future(SimpleValueColumn(kind, languageType, columnInformation))
  }

  private def mapAttachmentColumn(columnInformation: ColumnInformation): Future[AttachmentColumn] = {
    Future(AttachmentColumn(columnInformation))
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
    import scala.collection.JavaConverters._

    val columnId = row.get[ColumnId](0)
    val columnName = row.get[String](1)
    val kind = TableauxDbType(row.get[String](2))
    val ordering = row.get[Ordering](3)
    val identifier = row.get[Boolean](5)

    val languageType = LanguageType(Option(row.get[String](4))) match {
      case LanguageNeutral => LanguageNeutral
      case MultiLanguage => MultiLanguage
      case c: MultiCountry =>
        val codes = Option(row.get[String](6))
          .map(str => Json.fromArrayString(str).asScala.map({ case code: String => code }).toSeq)
          .getOrElse(Seq.empty[String])

        MultiCountry(CountryCodes(codes))
    }

    for {
      displayInfoSeq <- retrieveDisplayInfo(table, columnId)
      column <- mapColumn(depth,
                          kind,
                          languageType,
                          BasicColumnInformation(table, columnId, columnName, ordering, identifier, displayInfoSeq))
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
        val name = arr.getString(1)
        val description = arr.getString(2)

        if (name != null || description != null) {
          Seq(DisplayInfos.fromString(langtag, name, description))
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

        val table1 = res.getLong(0).toLong
        val table2 = res.getLong(1).toLong
        val linkId = res.getLong(2).toLong
        val cardinality1 = res.getLong(3).toInt
        val cardinality2 = res.getLong(4).toInt
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

      toTable <- tableStruc.retrieve(linkDirection.to)

    } yield (linkId, linkDirection, toTable)
  }

  def deleteLinkBothDirections(table: Table, columnId: ColumnId): Future[Unit] = {
    delete(table, columnId, bothDirections = true)
  }

  def delete(table: Table, columnId: ColumnId): Future[Unit] = delete(table, columnId, bothDirections = false)

  protected def delete(table: Table, columnId: ColumnId, bothDirections: Boolean): Future[Unit] = {

    // Retrieve all filter for columnId and check if columns is not empty
    // If columns is empty last column would be deleted => error
    for {
      columns <- retrieveAll(table)
        .filter(_.nonEmpty)
        .recoverWith({
          case _: NoSuchElementException =>
            Future.failed(NotFoundInDatabaseException("No column found at all", "no-column-found"))
        })

      _ <- Future
        .successful(columns)
        .filter(!_.forall(_.id == columnId))
        .recoverWith({
          case _: NoSuchElementException =>
            Future.failed(DatabaseException("Last column can't be deleted", "delete-last-column"))
        })

      column = columns
        .find(_.id == columnId)
        .getOrElse(throw NotFoundInDatabaseException("Column can't be deleted because it doesn't exist.",
                                                     "delete-non-existing"))

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
          t.query("DELETE FROM system_columns WHERE link_id = ?", Json.arr(linkId))
        } else {
          t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?",
                  Json.arr(column.id, column.table.id))
        }

        deleteFuture.flatMap({
          case (t, _) =>
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
    for {
      t <- connection.begin()

      (t, _) <- t.query("DELETE FROM system_attachment WHERE column_id = ? AND table_id = ?",
                        Json.arr(column.id, column.table.id))
      (t, _) <- t
        .query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(column.id, column.table.id))
        .map({ case (t, json) => (t, deleteNotNull(json)) })

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

      (t, _) <- t
        .query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
        .map({ case (t, json) => (t, deleteNotNull(json)) })

      _ <- t.commit()
    } yield ()
  }

  def change(
      table: Table,
      columnId: ColumnId,
      columnName: Option[String],
      ordering: Option[Ordering],
      kind: Option[TableauxDbType],
      identifier: Option[Boolean],
      displayInfos: Option[Seq[DisplayInfo]],
      countryCodes: Option[Seq[String]]
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

      _ <- Future(checkUpdateResults(resultName, resultOrdering, resultKind, resultIdentifier, resultCountryCodes))
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
