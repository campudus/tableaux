package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.{DatabaseException, NotFoundInDatabaseException}
import org.vertx.scala.core.json._

import scala.concurrent.Future


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
      case CreateSimpleColumn(name, orderingOpt, kind, languageType, identifier) =>
        createValueColumn(table.id, kind, name, orderingOpt, languageType, identifier).map {
          case (id, ordering) => Mapper(languageType, kind).apply(table, id, name, ordering, identifier)
        }

      case CreateLinkColumn(name, orderingOpt, toTableId, toName, singleDirection, identifier) => for {
        toTable <- tableStruc.retrieve(toTableId)
        toCol <- retrieveAll(toTable).map(_.head.asInstanceOf[ValueColumn[_]])
        (linkId, id, ordering) <- createLinkColumn(table, name, toName, toTableId, orderingOpt, singleDirection, identifier)
      } yield LinkColumn(table, id, toCol, linkId, LeftToRight(table.id, toTableId), name, ordering, identifier)

      case CreateAttachmentColumn(name, orderingOpt, identifier) =>
        createAttachmentColumn(table.id, name, orderingOpt, identifier).map {
          case (id, ordering) => AttachmentColumn(table, id, name, ordering, identifier)
        }
    }
  }

  private def createValueColumn(tableId: TableId, kind: TableauxDbType, name: String, ordering: Option[Ordering], languageType: LanguageType, identifier: Boolean): Future[(ColumnId, Ordering)] = {
    connection.transactional { t =>
      for {
        (t, resultJson) <- insertSystemColumn(t, tableId, name, kind, ordering, None, languageType, identifier)
        resultRow = insertNotNull(resultJson).head

        (t, _) <- languageType match {
          case MultiLanguage => t.query(s"ALTER TABLE user_table_lang_$tableId ADD column_${resultRow.get[ColumnId](0)} ${kind.toDbType}")
          case SingleLanguage => t.query(s"ALTER TABLE user_table_$tableId ADD column_${resultRow.get[ColumnId](0)} ${kind.toDbType}")
        }
      } yield {
        (t, (resultRow.get[ColumnId](0), resultRow.get[Ordering](1)))
      }
    }
  }

  private def createAttachmentColumn(tableId: TableId, name: String, ordering: Option[Ordering], identifier: Boolean): Future[(ColumnId, Ordering)] = {
    connection.transactional { t =>
      for {
        (t, result) <- insertSystemColumn(t, tableId, name, AttachmentType, ordering, None, SingleLanguage, identifier)
        result <- Future.successful(insertNotNull(result).head)
      } yield (t, (result.get[ColumnId](0), result.get[Ordering](1)))
    }
  }

  private def createLinkColumn(table: Table, name: String, toName: Option[String], toTableId: TableId, ordering: Option[Ordering], singleDirection: Boolean, identifier: Boolean): Future[(Long, ColumnId, Ordering)] = {
    val tableId = table.id

    connection.transactional { t =>
      for {
        (t, result) <- t.query("INSERT INTO system_link_table (table_id_1, table_id_2) VALUES (?, ?) RETURNING link_id", Json.arr(tableId, toTableId))
        linkId = insertNotNull(result).head.get[Long](0)

        // insert link column on source table
        (t, result) <- insertSystemColumn(t, tableId, name, LinkType, ordering, Some(linkId), SingleLanguage, identifier)

        // only add the second link column if tableId != toTableId or singleDirection is false
        (t, _) <- {
          if (!singleDirection && tableId != toTableId) {
            insertSystemColumn(t, toTableId, toName.getOrElse(table.name), LinkType, None, Some(linkId), SingleLanguage, identifier)
          } else {
            Future((t, Json.emptyObj()))
          }
        }

        (t, _) <- t.query(
          s"""
             |CREATE TABLE link_table_$linkId (
             |id_1 bigint,
             |id_2 bigint,
             |PRIMARY KEY(id_1, id_2),
             |CONSTRAINT link_table_${linkId}_foreign_1
             |FOREIGN KEY(id_1)
             |REFERENCES user_table_$tableId (id)
             |ON DELETE CASCADE,
             |CONSTRAINT link_table_${linkId}_foreign_2
             |FOREIGN KEY(id_2)
             |REFERENCES user_table_$toTableId (id)
             |ON DELETE CASCADE
             |)""".stripMargin)
      } yield {
        val json = insertNotNull(result).head

        (t, (linkId, json.get[ColumnId](0), json.get[Ordering](1)))
      }
    }
  }

  private def insertSystemColumn(t: connection.Transaction, tableId: TableId, name: String, kind: TableauxDbType, ordering: Option[Ordering], linkId: Option[Long], languageType: LanguageType, identifier: Boolean): Future[(connection.Transaction, JsonObject)] = {
    def insertStatement(tableId: TableId, ordering: String) =
      s"""INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id, multilanguage, identifier) VALUES (
          |?, nextval('system_columns_column_id_table_$tableId'), ?, ?, $ordering, ?, ?, ?) RETURNING column_id, ordering""".stripMargin

    for {
      (t, result) <- ordering match {
        case None => t.query(insertStatement(tableId, s"currval('system_columns_column_id_table_$tableId')"), Json.arr(tableId, kind.name, name, linkId.orNull, languageType.toBoolean, identifier))
        case Some(ord) => t.query(insertStatement(tableId, "?"), Json.arr(tableId, kind.name, name, ord, linkId.orNull, languageType.toBoolean, identifier))
      }
    } yield (t, result)
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
    val select = "SELECT column_id, user_column_name, column_type, ordering, multilanguage, identifier FROM system_columns WHERE table_id = ? AND column_id = ?"
    for {
      result <- {
        val json = connection.query(select, Json.arr(table.id, columnId))
        json.map(selectNotNull(_).head)
      }
      mappedColumn <- mapColumn(depth, table, result.get[ColumnId](0), result.get[String](1), Mapper.getDatabaseType(result.get[String](2)), result.get[Ordering](3), LanguageType(result.get[Boolean](4)), result.get[Boolean](5))
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

  private def concatColumnAndColumns(table: Table, concatColumns: Seq[ColumnType[_]], columns: Seq[ColumnType[_]]): Seq[ColumnType[_]] = {
    concatColumns.size match {
      case x if x >= 2 =>
        // in case of two or more identifier columns we preserve the order of column
        // and a concatcolumn in front of all columns
        columns.+:(ConcatColumn(table, "ID", concatColumns))
      case x if x == 1 =>
        // in case of one identifier column we don't get a concat column
        // but the identifier column will be the first
        columns.sortBy(_.identifier)(Ordering[Boolean].reverse)
      case _ =>
        // no identifier -> return columns
        columns
    }
  }

  private def retrieveColumns(table: Table, depth: Int, identifierOnly: Boolean): Future[(Seq[ColumnType[_]], Seq[ColumnType[_]])] = {
    val identCondition = if (identifierOnly) " AND identifier IS TRUE " else ""
    val select =
      s"""SELECT column_id, user_column_name, column_type, ordering, multilanguage, identifier
          | FROM system_columns
          | WHERE table_id = ? $identCondition ORDER BY ordering, column_id""".stripMargin

    for {
      result <- connection.query(select, Json.arr(table.id))
      mappedColumns <- {
        val futures = getSeqOfJsonArray(result).map { arr =>
          val columnId = arr.get[ColumnId](0)
          val columnName = arr.get[String](1)
          val kind = Mapper.getDatabaseType(arr.get[String](2))
          val ordering = arr.get[Ordering](3)
          val languageType = LanguageType(arr.get[Boolean](4))
          val identifier = arr.get[Boolean](5)

          mapColumn(depth, table, columnId, columnName, kind, ordering, languageType, identifier)
        }
        Future.sequence(futures)
      }
    } yield (mappedColumns.filter(_.identifier), mappedColumns)
  }

  private def mapColumn(depth: Int, table: Table, columnId: ColumnId, columnName: String, kind: TableauxDbType, ordering: Ordering, languageType: LanguageType, identifier: Boolean): Future[ColumnType[_]] = {
    kind match {
      case AttachmentType => mapAttachmentColumn(table, columnId, columnName, ordering, identifier)
      case LinkType => mapLinkColumn(depth, table, columnId, columnName, ordering, identifier)

      case kind: TableauxDbType => mapValueColumn(table, columnId, columnName, kind, ordering, languageType, identifier)
    }
  }

  private def mapValueColumn(table: Table, columnId: ColumnId, columnName: String, kind: TableauxDbType, ordering: Ordering, languageType: LanguageType, identifier: Boolean): Future[ColumnType[_]] = {
    Future(Mapper(languageType, kind).apply(table, columnId, columnName, ordering, identifier))
  }

  private def mapAttachmentColumn(table: Table, columnId: ColumnId, columnName: String, ordering: Ordering, identifier: Boolean): Future[AttachmentColumn] = {
    Future(AttachmentColumn(table, columnId, columnName, ordering, identifier))
  }

  private def mapLinkColumn(depth: Int, fromTable: Table, linkColumnId: ColumnId, columnName: String, ordering: Ordering, identifier: Boolean): Future[LinkColumn[_]] = {
    for {
      (linkId, linkDirection, toTable) <- getLinkInformation(fromTable, linkColumnId)

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
        throw new NotFoundInDatabaseException(s"Link points at table ${toTable.id} without columns", "not_found")
      }

      val toColumn = toColumnOpt.get.asInstanceOf[ValueColumn[_]]
      LinkColumn(fromTable, linkColumnId, toColumn, linkId, linkDirection, columnName, ordering, identifier)
    }
  }

  private def getLinkInformation(fromTable: Table, columnId: ColumnId): Future[(LinkId, LinkDirection, Table)] = {
    for {
      result <- connection.query(
        """
          |SELECT
          | table_id_1,
          | table_id_2,
          | link_id
          |FROM system_link_table
          |WHERE link_id = (
          |    SELECT link_id
          |    FROM system_columns
          |    WHERE table_id = ? AND column_id = ?
          |)""".stripMargin, Json.arr(fromTable.id, columnId))

      (linkId, linkDirection) = {
        val res = selectNotNull(result).head

        val table1 = res.getLong(0).toLong
        val table2 = res.getLong(1).toLong
        val linkId = res.getLong(2).toLong

        (linkId, LinkDirection(fromTable.id, table1, table2))
      }

      toTable <- tableStruc.retrieve(linkDirection.to)

    } yield (linkId, linkDirection, toTable)
  }

  def delete(table: Table, columnId: ColumnId): Future[Unit] = {
    // Retrieve all filter for columnId and check if columns is not empty
    // If columns is empty last column would be deleted => error
    for {
      columns <- retrieveAll(table)
        .filter(_.nonEmpty)
        .recoverWith({ case _ => Future.failed(NotFoundInDatabaseException("No column found at all", "no-column-found")) })

      _ <- Future.successful(columns)
        .filter(!_.forall(_.id == columnId))
        .recoverWith({ case _ => Future.failed(DatabaseException("Last column can't be deleted", "delete-last-column")) })

      column = columns
        .find(_.id == columnId)
        .getOrElse(throw NotFoundInDatabaseException("Column can't be deleted because it doesn't exist.", "delete-non-existing"))

      _ <- {
        column match {
          case c: ConcatColumn => Future.failed(DatabaseException("ConcatColumn can't be deleted", "delete-concat"))
          case c: LinkColumn[_] => deleteLink(c)
          case c: AttachmentColumn => deleteAttachment(c)
          case c: ColumnType[_] => deleteSimpleColumn(c)
        }
      }
    } yield ()
  }

  private def deleteLink(column: LinkColumn[_]): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(column.id, column.table.id))
      linkId <- Future(selectCheckSize(result, 1).head.get[Long](0)) recoverWith t.rollbackAndFail()

      (t, result) <- t.query("DELETE FROM system_columns WHERE link_id = ?", Json.arr(linkId))
      _ <- Future(deleteCheckSize(result, 2)) recoverWith t.rollbackAndFail()

      (t, _) <- t.query(s"DROP TABLE IF EXISTS link_table_$linkId")

      _ <- t.commit()
    } yield ()
  }

  private def deleteAttachment(column: AttachmentColumn): Future[Unit] = {
    for {
      t <- connection.begin()

      (t, _) <- t.query("DELETE FROM system_attachment WHERE column_id = ? AND table_id = ?", Json.arr(column.id, column.table.id))
      (t, _) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(column.id, column.table.id)).map({ case (t, json) => (t, deleteNotNull(json)) })

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

      (t, _) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId)).map({ case (t, json) => (t, deleteNotNull(json)) })

      _ <- t.commit()
    } yield ()
  }

  def change(table: Table, columnId: ColumnId, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType], identifier: Option[Boolean]): Future[ColumnType[_]] = {
    val tableId = table.id

    for {
      t <- connection.begin()

      (t, resultName) <- optionToValidFuture(columnName, t, { name: String => t.query(s"UPDATE system_columns SET user_column_name = ? WHERE table_id = ? AND column_id = ?", Json.arr(name, tableId, columnId)) })
      (t, resultOrdering) <- optionToValidFuture(ordering, t, { ord: Ordering => t.query(s"UPDATE system_columns SET ordering = ? WHERE table_id = ? AND column_id = ?", Json.arr(ord, tableId, columnId)) })
      (t, resultKind) <- optionToValidFuture(kind, t, { k: TableauxDbType => t.query(s"UPDATE system_columns SET column_type = ? WHERE table_id = ? AND column_id = ?", Json.arr(k.name, tableId, columnId)) })
      (t, resultIdentifier) <- optionToValidFuture(identifier, t, { ident: Boolean => t.query(s"UPDATE system_columns SET identifier = ? WHERE table_id = ? AND column_id = ?", Json.arr(ident, tableId, columnId)) })

      (t, _) <- optionToValidFuture(kind, t, { k: TableauxDbType => t.query(s"ALTER TABLE user_table_$tableId ALTER COLUMN column_$columnId TYPE ${k.toDbType} USING column_$columnId::${k.toDbType}") })
        .recoverWith(t.rollbackAndFail())

      _ <- Future(checkUpdateResults(resultName, resultOrdering, resultKind, resultIdentifier))
        .recoverWith(t.rollbackAndFail())

      _ <- t.commit()

      column <- retrieve(table, columnId)
    } yield column
  }
}
