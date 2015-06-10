package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseQuery, DatabaseConnection, Mapper, TableauxDbType}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

class ColumnStructure(val connection: DatabaseConnection) extends DatabaseQuery {

  def insert(tableId: IdType, dbType: TableauxDbType, name: String, ordering: Option[Ordering]): Future[(IdType, Ordering)] = for {
    t <- connection.begin()
    (t, result) <- ordering match {
      case None => t.query(s"""
                     |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering)
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, currval('system_columns_column_id_table_$tableId'))
                     |  RETURNING column_id, ordering""".stripMargin,
        Json.arr(tableId, dbType.toString, name))
      case Some(ord) => t.query(s"""
                     |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering)
                     |  VALUES (?, nextval('system_columns_column_id_table_$tableId'), ?, ?, ?)
                     |  RETURNING column_id, ordering""".stripMargin,
        Json.arr(tableId, dbType.toString, name, ord))
    }
    result <- Future.successful(insertNotNull(result).head)
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId ADD column_${result.get[IdType](0)} $dbType")
    _ <- t.commit()
  } yield (result.get[IdType](0), result.get[Ordering](1))

  def insertLink(tableId: IdType, name: String, fromColumnId: IdType, toTableId: IdType, toColumnId: IdType, ordering: Option[Ordering]): Future[(IdType, Ordering)] = for {
    t <- connection.begin()
    (t, result) <- t.query(s"""INSERT INTO system_link_table (table_id_1, table_id_2, column_id_1, column_id_2) VALUES (?, ?, ?, ?) RETURNING link_id""".stripMargin,
      Json.arr(tableId, toTableId, fromColumnId, toColumnId))
    linkId <- Future.successful { insertNotNull(result).head.get[IdType](0) }
    (t, _) <- t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?,
                    |  nextval('system_columns_column_id_table_$toTableId'),
                    |  'link',
                    |  ?,
                    |  currval('system_columns_column_id_table_$toTableId'),
                    |  ?)""".stripMargin,
      Json.arr(toTableId, name, linkId))
    (t, result) <- ordering match {
      case None => t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?,
                    |  nextval('system_columns_column_id_table_$tableId'),
                    |  'link',
                    |  ?,
                    |  currval('system_columns_column_id_table_$tableId'),
                    |  ?
                    |) RETURNING column_id, ordering""".stripMargin,
        Json.arr(tableId, name, linkId))
      case Some(ord) => t.query(s"""
                    |INSERT INTO system_columns (table_id, column_id, column_type, user_column_name, ordering, link_id) VALUES (
                    |  ?,
                    |  nextval('system_columns_column_id_table_$tableId'),
                    |  'link',
                    |  ?,
                    |  ?,
                    |  ?
                    |) RETURNING column_id, ordering""".stripMargin,
        Json.arr(tableId, name, ord, linkId))
    }
    (t, _) <- t.query(s"""
                    |CREATE TABLE link_table_$linkId (
                    |  id_1 bigint,
                    |  id_2 bigint,
                    |  PRIMARY KEY(id_1, id_2),
                    |  CONSTRAINT link_table_${linkId}_foreign_1
                    |    FOREIGN KEY(id_1)
                    |    REFERENCES user_table_$tableId (id)
                    |    ON DELETE CASCADE,
                    |  CONSTRAINT link_table_${linkId}_foreign_2
                    |    FOREIGN KEY(id_2)
                    |    REFERENCES user_table_$toTableId (id)
                    |    ON DELETE CASCADE
                    |)""".stripMargin)
    _ <- t.commit()
  } yield {
    val json = insertNotNull(result).head
    (json.get[IdType](0), json.get[Ordering](1))
  }

  def get(tableId: IdType, columnId: IdType): Future[(IdType, String, TableauxDbType, Ordering)] = for {
    result <- connection.query("""
                              |SELECT column_id, user_column_name, column_type, ordering
                              |  FROM system_columns
                              |  WHERE table_id = ? AND column_id = ?
                              |  ORDER BY column_id""".stripMargin, Json.arr(tableId, columnId))
  } yield {
    val json = selectNotNull(result).head
    (json.get[IdType](0), json.get[String](1), Mapper.getDatabaseType(json.get[String](2)), json.get[Ordering](3))
  }

  def getAll(tableId: IdType): Future[Seq[(IdType, String, TableauxDbType, Ordering)]] = {
    connection.query("SELECT column_id, user_column_name, column_type, ordering FROM system_columns WHERE table_id = ? ORDER BY column_id", Json.arr(tableId))
  } map { getSeqOfJsonArray(_) map { arr => (arr.get[IdType](0), arr.get[String](1), Mapper.getDatabaseType(arr.get[String](2)), arr.get[Ordering](3)) } }

  def getToColumn(tableId: IdType, columnId: IdType): Future[(IdType, IdType)] = {
    for {
      result <- connection.query("""
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
        if (tableId == res.get[IdType](0)) {
          (res.get[IdType](1), res.get[IdType](3))
        } else {
          (res.get[IdType](0), res.get[IdType](2))
        }
      }
    } yield (toTableId, toColumnId)
  }

  def delete(tableId: IdType, columnId: IdType): Future[Unit] = for {
    t <- connection.begin()
    (t, _) <- t.query(s"ALTER TABLE user_table_$tableId DROP COLUMN IF EXISTS column_$columnId")
    (t, result) <- t.query("DELETE FROM system_columns WHERE column_id = ? AND table_id = ?", Json.arr(columnId, tableId))
    _ <- Future.apply(deleteNotNull(result)) recoverWith t.rollbackAndFail()
    _ <- t.commit()
  } yield ()

  def change(tableId: IdType, columnId: IdType, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[Unit] = for {
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
