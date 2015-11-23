package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

class CellModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def update[A](table: Table, column: ColumnType[_], rowId: RowId, value: A): Future[Unit] = {
    for {
      _ <- connection.query(s"UPDATE user_table_${table.id} SET column_${column.id} = ? WHERE id = ?", Json.arr(value, rowId))
    } yield ()
  }

  def updateLink(table: Table, column: LinkColumn[_], fromId: RowId, toId: RowId): Future[Unit] = {
    val linkId = column.linkInformation._1
    val id1 = column.linkInformation._2
    val id2 = column.linkInformation._3

    for {
      _ <- connection.query(s"INSERT INTO link_table_$linkId($id1, $id2) VALUES (?, ?)", Json.arr(fromId, toId))
    } yield ()
  }

  def updateTranslations(table: Table, column: ColumnType[_], rowId: RowId, values: Seq[(String, _)]): Future[JsonObject] = {
    val tableId = table.id
    val columnId = column.id

    val select = s"SELECT COUNT(id) FROM user_table_lang_$tableId WHERE id = ? AND langtag = ?"
    val insert = s"INSERT INTO user_table_lang_$tableId(id, langtag, column_$columnId) VALUES(?, ?, ?)"
    val update = s"UPDATE user_table_lang_$tableId SET column_$columnId = ? WHERE id = ? AND langtag = ?"

    connection.transactionalFoldLeft(values) { (t, _, value) =>
      for {
        (t, count) <- t.query(select, Json.arr(rowId, value._1)).map({
          case (t, json) =>
            (t, selectNotNull(json).head.get[Long](0))
        })

        (t, result) <- if (count > 0) {
          t.query(update, Json.arr(value._2, rowId, value._1))
        } else {
          t.query(insert, Json.arr(rowId, value._1, value._2))
        }
      } yield (t, result)
    }
  }

  def putLinks(table: Table, column: LinkColumn[_], fromId: RowId, toIds: Seq[RowId]): Future[Unit] = {
    val linkId = column.linkInformation._1
    val id1 = column.linkInformation._2
    val id2 = column.linkInformation._3

    val paramStr = toIds.map(_ => "(?, ?)").mkString(", ")
    val params = toIds.flatMap(List(fromId, _))

    for {
      t <- connection.begin()

      (t, _) <- t.query(s"DELETE FROM link_table_$linkId WHERE $id1 = ?", Json.arr(fromId))

      (t, _) <- {
        if (params.nonEmpty) {
          t.query(s"INSERT INTO link_table_$linkId($id1, $id2) VALUES $paramStr", Json.arr(params: _*))
        } else {
          Future((t, Json.emptyObj()))
        }
      }
      _ <- t.commit()
    } yield ()
  }
}
