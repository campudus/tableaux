package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.HelperFunctions._
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

class ColumnModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def createColumns(table: Table, createColumns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = Future.sequence(createColumns.map(createColumn(table, _)))
  
  def createColumn(table: Table, createColumn: CreateColumn): Future[ColumnType[_]] = {
    createColumn match {
      case CreateSimpleColumn(name, ordering, kind, languageType) =>
        createValueColumn(table.id, kind, name, ordering, languageType).map {
          case (id, ordering) => Mapper(languageType, kind).apply(table, id, name, ordering)
        }

      case CreateLinkColumn(name, ordering, linkConnection) => for {
        toCol <- retrieve(linkConnection.toTableId, linkConnection.toColumnId).asInstanceOf[Future[SimpleValueColumn[_]]] 
        (id, ordering) <- createLinkColumn(table.id, name, linkConnection, ordering)
      } yield LinkColumn(table, id, toCol, name, ordering)

      case CreateAttachmentColumn(name, ordering) =>
        createAttachmentColumn(table.id, name, ordering).map {
          case (id, ordering) => AttachmentColumn(table, id, name, ordering)
        }
    }
  }
  
  def createValueColumn(tableId: TableId, dbType: TableauxDbType, name: String, ordering: Option[Ordering], languageType: LanguageType): Future[(ColumnId, Ordering)] = {
    connection.transactional { t =>
      for {
        (t, result) <- insertSystemColumn(t, tableId, name, dbType, ordering, None, languageType)
        result <- Future.successful(insertNotNull(result).head)

        (t, _) <- languageType match {
          case MultiLanguage => t.query(s"ALTER TABLE user_table_lang_$tableId ADD column_${result.get[ColumnId](0)} $dbType")
          case SingleLanguage => t.query(s"ALTER TABLE user_table_$tableId ADD column_${result.get[ColumnId](0)} $dbType")
        }
      } yield (t, (result.get[ColumnId](0), result.get[Ordering](1)))
    }
  }

  def createAttachmentColumn(tableId: TableId, name: String, ordering: Option[Ordering]): Future[(ColumnId, Ordering)] = {
    connection.transactional { t =>
      for {
        (t, result) <- insertSystemColumn(t, tableId, name, AttachmentType, ordering, None, SingleLanguage)
        result <- Future.successful(insertNotNull(result).head)
      } yield (t, (result.get[ColumnId](0), result.get[Ordering](1)))
    }
  }

  def createLinkColumn(tableId: TableId, name: String, link: LinkConnection, ordering: Option[Ordering]): Future[(ColumnId, Ordering)] = {
    val fromColumnId = link.fromColumnId
    val toTableId = link.toTableId
    val toColumnId = link.toColumnId

    connection.transactional { t =>
      for {
        (t, result) <- t.query("INSERT INTO system_link_table (table_id_1, table_id_2, column_id_1, column_id_2) VALUES (?, ?, ?, ?) RETURNING link_id", Json.arr(tableId, toTableId, fromColumnId, toColumnId))
        linkId <- Future(insertNotNull(result).head.get[Long](0))

        // only add the two link column if tableId != toTableId
        (t, _) <- {
          if (tableId != toTableId) {
            insertSystemColumn(t, toTableId, name, LinkType, None, Some(linkId), SingleLanguage)
          } else {
            Future((t, Json.emptyObj()))
          }
        }
        (t, result) <- insertSystemColumn(t, tableId, name, LinkType, ordering, Some(linkId), SingleLanguage)

        (t, _) <- t.query( s"""
                              |CREATE TABLE link_table_$linkId (
                              | id_1 bigint,
                              | id_2 bigint,
                              | PRIMARY KEY(id_1, id_2),
                              | CONSTRAINT link_table_${linkId}_foreign_1
                              |   FOREIGN KEY(id_1)
                              |   REFERENCES user_table_$tableId (id)
                              |   ON DELETE CASCADE,
                              | CONSTRAINT link_table_${linkId}_foreign_2
                              |   FOREIGN KEY(id_2)
                              |   REFERENCES user_table_$toTableId (id)
                              |   ON DELETE CASCADE
                              |)""".stripMargin)
      } yield {
        val json = insertNotNull(result).head

        (t, (json.get[ColumnId](0), json.get[Ordering](1)))
      }
    }
  }

  private def insertSystemColumn(t: connection.Transaction, tableId: TableId, name: String, kind: TableauxDbType, ordering: Option[Ordering], linkId: Option[Long], languageType: LanguageType): Future[(connection.Transaction, JsonObject)] = {
    def insertStatement(tableId: TableId, ordering: String) =
      s"""INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id, multilanguage) VALUES (
          | ?, nextval('system_columns_column_id_table_$tableId'), ?, ?, $ordering, ?, ?) RETURNING column_id, ordering""".stripMargin

    for {
      (t, result) <- ordering match {
        case None => t.query(insertStatement(tableId, s"currval('system_columns_column_id_table_$tableId')"), Json.arr(tableId, kind.name, name, linkId.orNull, languageType.toBoolean))
        case Some(ord) => t.query(insertStatement(tableId, "?"), Json.arr(tableId, kind.name, name, ord, linkId.orNull, languageType.toBoolean))
      }
    } yield (t, result)
  }

  def retrieve(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = {
    val select = "SELECT column_id, user_column_name, column_type, ordering, multilanguage FROM system_columns WHERE table_id = ? AND column_id = ?"
    for {
      result <- {
        val json = connection.query(select, Json.arr(tableId, columnId))
        json.map(selectNotNull(_).head)
      }
      mappedColumn <- mapColumn(tableId, result.get[ColumnId](0), result.get[String](1), Mapper.getDatabaseType(result.get[String](2)), result.get[Ordering](3), LanguageType(result.get[Boolean](4)))
    } yield mappedColumn
  }

  def retrieveAll(tableId: TableId): Future[Seq[ColumnType[_]]] = {
    val select = "SELECT column_id, user_column_name, column_type, ordering, multilanguage FROM system_columns WHERE table_id = ? ORDER BY column_id"
    for {
      result <- connection.query(select, Json.arr(tableId))
      mappedColumns <- {
        val futures = getSeqOfJsonArray(result).map { arr =>
          val columnId = arr.get[ColumnId](0)
          val columnName = arr.get[String](1)
          val kind = Mapper.getDatabaseType(arr.get[String](2))
          val ordering = arr.get[Ordering](3)
          val languageType = LanguageType(arr.get[Boolean](4))

          mapColumn(tableId, columnId, columnName, kind, ordering, languageType)
        }
        Future.sequence(futures)
      }
    } yield mappedColumns
  }

  private def mapColumn(tableId: TableId, columnId: ColumnId, columnName: String, kind: TableauxDbType, ordering: Ordering, languageType: LanguageType): Future[ColumnType[_]] = {
    val table = Table(tableId, "")

    kind match {
      case AttachmentType => mapAttachmentColumn(table, columnId, columnName, ordering)
      case LinkType => mapLinkColumn(table, columnId, columnName, ordering)

      case kind: TableauxDbType => mapValueColumn(table, columnId, columnName, kind, ordering, languageType)
    }
  }

  private def mapValueColumn(table: Table, columnId: ColumnId, columnName: String, columnKind: TableauxDbType, ordering: Ordering, languageType: LanguageType): Future[ColumnType[_]] = {
    Future(Mapper(languageType, columnKind).apply(table, columnId, columnName, ordering))
  }

  private def mapAttachmentColumn(table: Table, columnId: ColumnId, columnName: String, ordering: Ordering): Future[AttachmentColumn] = {
    Future(AttachmentColumn(table, columnId, columnName, ordering))
  }

  private def mapLinkColumn(fromTable: Table, linkColumnId: ColumnId, columnName: String, ordering: Ordering): Future[LinkColumn[_]] = {
    for {
      (toTableId, toColumnId) <- getToColumn(fromTable.id, linkColumnId)
      toCol <- retrieve(toTableId, toColumnId).asInstanceOf[Future[SimpleValueColumn[_]]]
    } yield {
      LinkColumn(fromTable, linkColumnId, toCol, columnName, ordering)
    }
  }

  private def getToColumn(tableId: TableId, columnId: ColumnId): Future[(TableId, ColumnId)] = {
    for {
      result <- connection.query( """
                                    |SELECT table_id_1, table_id_2, column_id_1, column_id_2
                                    |  FROM system_link_table
                                    |  WHERE link_id = (
                                    |    SELECT link_id
                                    |    FROM system_columns
                                    |    WHERE table_id = ? AND column_id = ?
                                    |  )""".stripMargin, Json.arr(tableId, columnId))
      (toTableId, toColumnId) <- Future.successful {
        val res = selectNotNull(result).head

        /* we need this because links can go both ways */
        if (tableId == res.get[TableId](0)) {
          (res.get[TableId](1), res.get[ColumnId](3))
        } else {
          (res.get[TableId](0), res.get[ColumnId](2))
        }
      }
    } yield (toTableId, toColumnId)
  }

  def delete(tableId: TableId, columnId: ColumnId): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId")
    (t, _) <- t.query(s"ALTER TABLE user_table_lang_$tableId DROP COLUMN IF EXISTS column_$columnId")
    (t, result) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
    _ <- Future.apply(deleteNotNull(result)) recoverWith t.rollbackAndFail()
    _ <- t.commit()
  } yield ()

  def change(tableId: TableId, columnId: ColumnId, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[Unit] = for {
    t <- connection.begin()
    (t, result1) <- optionToValidFuture(columnName, t, { name: String => t.query(s"UPDATE system_columns SET user_column_name = ? WHERE table_id = ? AND column_id = ?", Json.arr(name, tableId, columnId)) })
    (t, result2) <- optionToValidFuture(ordering, t, { ord: Ordering => t.query(s"UPDATE system_columns SET ordering = ? WHERE table_id = ? AND column_id = ?", Json.arr(ord, tableId, columnId)) })
    (t, result3) <- optionToValidFuture(kind, t, { k: TableauxDbType => t.query(s"UPDATE system_columns SET column_type = ? WHERE table_id = ? AND column_id = ?", Json.arr(k.toString, tableId, columnId)) })
    (t, _) <- optionToValidFuture(kind, t, { k: TableauxDbType => t.query(s"ALTER TABLE user_table_$tableId ALTER COLUMN column_$columnId TYPE ${k.toString} USING column_$columnId::${k.toString}") })
    _ <- Future.apply(checkUpdateResults(Seq(result1, result2, result3))) recoverWith t.rollbackAndFail()
    _ <- t.commit()
  } yield ()

  private def checkUpdateResults(seq: Seq[JsonObject]): Unit = seq map {
    json => if (json.containsField("message")) updateNotNull(json)
  }

  private def optionToValidFuture[A, B](opt: Option[A], trans: B, someCase: A => Future[(B, JsonObject)]): Future[(B, JsonObject)] = opt match {
    case Some(x) => someCase(x)
    case None => Future.successful(trans, Json.obj())
  }
}
