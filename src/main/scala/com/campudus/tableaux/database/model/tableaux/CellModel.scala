package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.domain.{SimpleValueColumn, MultiLanguageColumn, LinkColumn, ColumnType}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._
import com.campudus.tableaux.helper.HelperFunctions
import HelperFunctions._

import scala.concurrent.Future

class CellModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def update[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Unit] = {
    connection.query(s"UPDATE user_table_$tableId SET column_$columnId = ? WHERE id = ?", Json.arr(value, rowId))
  } map { _ => () }

  def updateLink(tableId: TableId, linkColumnId: ColumnId, leftRow: RowId, rightRow: RowId): Future[Unit] = for {
    t <- connection.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
    linkId <- Future.successful(selectNotNull(result).head.get[Long](0))
    (t, _) <- t.query(s"INSERT INTO link_table_$linkId VALUES (?, ?)", Json.arr(leftRow, rightRow))
    _ <- t.commit()
  } yield ()

  def updateTranslations(tableId: TableId, columnId: ColumnId, rowId: RowId, values: Seq[(String, _)]): Future[JsonObject] = {
    val delete = s"DELETE FROM user_table_lang_$tableId WHERE id = ? AND langtag = ?"
    val insert = s"INSERT INTO user_table_lang_$tableId(id, langtag, column_$columnId) VALUES(?, ?, ?)"

    connection.transactional(values) { (t, _, value) =>
      // first, delete all translations
      t.query(delete, Json.arr(rowId, value._1)).flatMap {
        case (t, _) =>
          // insert new translation
          t.query(insert, Json.arr(rowId, value._1, value._2))
      }
    }
  }

  def putLinks(tableId: TableId, linkColumnId: ColumnId, from: RowId, tos: Seq[RowId]): Future[Unit] = {
    val paramStr = tos.map(_ => "(?, ?)").mkString(", ")
    val params = tos.flatMap(List(from, _))

    for {
      t <- connection.begin()
      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
      linkId <- Future.successful(selectNotNull(result).head.get[Long](0))
      (t, _) <- t.query(s"DELETE FROM link_table_$linkId")
      (t, _) <- {
        if (params.nonEmpty) {
          t.query(s"INSERT INTO link_table_$linkId VALUES $paramStr", Json.arr(params: _*))
        } else {
          Future.successful((t, Json.emptyObj()))
        }
      }
      _ <- t.commit()
    } yield ()
  }

  def getValue(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Any] = {
    connection.query(s"SELECT column_$columnId FROM user_table_$tableId WHERE id = ?", Json.arr(rowId)) map { selectNotNull(_).head.get[Any](0) }
  }

  def getTranslations(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[JsonObject] = {
    {
      connection.query(s"SELECT json_object_agg(DISTINCT COALESCE(langtag, 'de_DE'), column_$columnId) AS column_$columnId FROM user_table_lang_$tableId WHERE id = ? GROUP BY id", Json.arr(rowId))
    } map { r =>
      val result = getSeqOfJsonArray(r)

      if (result.isEmpty) {
        "{\"de_DE\": null}"
      } else {
        result.head.get[String](0)
      }
    } map { Json.fromObjectString }
  }

  def getLinkValues[A](linkColumn: LinkColumn[A], rowId: RowId): Future[Seq[JsonObject]] = {
    val tableId = linkColumn.table.id
    val linkColumnId = linkColumn.id

    val toTableId = linkColumn.to.table.id
    val toColumnId = linkColumn.to.id

    for {
      t <- connection.begin()

      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))

      linkId <- Future.successful(selectNotNull(result).head.get[Long](0))

      (t, result) <- t.query("SELECT table_id_1, table_id_2, column_id_1, column_id_2 FROM system_link_table WHERE link_id = ?", Json.arr(linkId))

      (id1, id2) <- Future.successful {
        val res = selectNotNull(result).head
        val linkTo2 = (res.get[TableId](1), res.get[ColumnId](3))

        if (linkTo2 == (toTableId, toColumnId)) {
          ("id_1", "id_2")
        } else {
          ("id_2", "id_1")
        }
      }

      (t, result) <- linkColumn.to match {
        case c: MultiLanguageColumn[_] => t.query(s"""
                                                     |SELECT to_table.id, json_object_agg(DISTINCT COALESCE(langtag, 'de_DE'), to_table_lang.column_$toColumnId) AS column_$toColumnId
                                                     |  FROM user_table_$tableId AS from_table
                                                     |  JOIN link_table_$linkId
                                                     |    ON from_table.id = link_table_$linkId.$id1
                                                     |  JOIN user_table_$toTableId AS to_table
                                                     |    ON to_table.id = link_table_$linkId.$id2
                                                     |  LEFT JOIN user_table_lang_$toTableId AS to_table_lang
                                                     |    ON to_table.id = to_table_lang.id
                                                     |WHERE from_table.id = ?
                                                     |GROUP BY to_table.id""".stripMargin, Json.arr(rowId))
        case c: SimpleValueColumn[_] => t.query(s"""
                                                   |SELECT to_table.id, to_table.column_$toColumnId
                                                   |  FROM user_table_$tableId AS from_table
                                                   |  JOIN link_table_$linkId
                                                   |    ON from_table.id = link_table_$linkId.$id1
                                                   |  JOIN user_table_$toTableId AS to_table
                                                   |    ON to_table.id = link_table_$linkId.$id2
                                                   |WHERE from_table.id = ?""".stripMargin, Json.arr(rowId))
      }



      _ <- t.commit()
    } yield {
      getSeqOfJsonArray(result) map {
        row =>
          linkColumn.to match {
            case c: MultiLanguageColumn[_] => Json.obj("id" -> row.get[Long](0), "value" -> Json.fromObjectString(row.get[String](1)))
            case c: SimpleValueColumn[_] => Json.obj("id" -> row.get[Long](0), "value" -> row.get[A](1))
          }
      }
    }
  }
}
