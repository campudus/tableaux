package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._
import com.campudus.tableaux.helper.HelperFunctions
import HelperFunctions._

import scala.concurrent.Future

class CellStructure(val connection: DatabaseConnection) extends DatabaseQuery {

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

  def updateTranslation(tableId: TableId, columnId: ColumnId, rowId: RowId, values: Seq[(String, _)]): Future[JsonObject] = {
    val delete = s"DELETE FROM user_table_lang_$tableId WHERE id = ? AND langtag = ?"
    val insert = s"INSERT INTO user_table_lang_$tableId(id, langtag, column_$columnId) VALUES(?, ?, ?)"

    connection.transactional[JsonObject]({ t =>
      values.foldLeft(Future(t, Json.emptyObj())) {
        (result, value) =>
        result.flatMap {
          case (t, obj) =>
            t.query(delete, Json.arr(rowId, value._1)).flatMap {
              case (t, obj) =>
                t.query(insert, Json.arr(rowId, value._1, value._2))
            }
        }
      }
    })
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

  def getLinkValues(tableId: TableId, linkColumnId: ColumnId, rowId: RowId, toTableId: TableId, toColumnId: ColumnId): Future[Seq[JsonObject]] = {
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

      (t, result) <- t.query(s"""
        |SELECT to_table.id, to_table.column_$toColumnId
        |  FROM user_table_$tableId AS from_table
        |  JOIN link_table_$linkId
        |    ON from_table.id = link_table_$linkId.$id1
        |  JOIN user_table_$toTableId AS to_table
        |    ON to_table.id = link_table_$linkId.$id2
        |WHERE from_table.id = ?""".stripMargin, Json.arr(rowId))

      _ <- t.commit()
    } yield {
      import ResultChecker._
      getSeqOfJsonArray(result) map {
        row =>
          val id = row.get[Any](0)
          val value = row.get[Any](1)
          Json.obj("id" -> id, "value" -> value)
      }
    }
  }
}
