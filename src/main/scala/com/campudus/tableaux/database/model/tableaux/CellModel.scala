package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.domain.{LinkColumn, MultiLanguageColumn, SimpleValueColumn}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

class CellModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def update[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Unit] = {
    for {
      _ <- connection.query(s"UPDATE user_table_$tableId SET column_$columnId = ? WHERE id = ?", Json.arr(value, rowId))
    } yield ()
  }

  def updateLink(tableId: TableId, linkColumnId: ColumnId, fromId: RowId, toId: RowId): Future[Unit] = for {
    t <- connection.begin()

    (t, linkId, id1, id2) <- getLinkInformation(t)(tableId, linkColumnId)

    _ <- connection.query(s"INSERT INTO link_table_$linkId($id1, $id2) VALUES (?, ?)", Json.arr(fromId, toId))
  } yield ()

  def updateTranslations(tableId: TableId, columnId: ColumnId, rowId: RowId, values: Seq[(String, _)]): Future[JsonObject] = {
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

  def putLinks(tableId: TableId, linkColumnId: ColumnId, fromId: RowId, toIds: Seq[RowId]): Future[Unit] = {
    val paramStr = toIds.map(_ => "(?, ?)").mkString(", ")
    val params = toIds.flatMap(List(fromId, _))

    for {
      t <- connection.begin()

      (t, linkId, id1, id2) <- getLinkInformation(t)(tableId, linkColumnId)

      (t, _) <- t.query(s"DELETE FROM link_table_$linkId WHERE $id1 = ?", Json.arr(fromId))
      (t, _) <- {
        if (params.nonEmpty) {
          t.query(s"INSERT INTO link_table_$linkId($id1, $id2) VALUES $paramStr", Json.arr(params: _*))
        } else {
          Future.successful((t, Json.emptyObj()))
        }
      }
      _ <- t.commit()
    } yield ()
  }

  /*
   * TODO should fetch the column and hand it over to the CellModel
   */
  @Deprecated
  private def getLinkInformation(t: connection.Transaction)(tableId: TableId, linkColumnId: ColumnId): Future[(connection.Transaction, Long, String, String)] = {
    for {
      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))

      linkId <- Future.successful(selectNotNull(result).head.get[Long](0))

      (t, result) <- t.query("SELECT table_id_1, table_id_2, column_id_1, column_id_2 FROM system_link_table WHERE link_id = ?", Json.arr(linkId))

      (id1, id2) <- Future.successful {
        val res = selectNotNull(result).head

        val table1 = res.get[TableId](0)

        if (tableId == table1) {
          ("id_1", "id_2")
        } else {
          ("id_2", "id_1")
        }
      }
    } yield (t, linkId, id1, id2)
  }

  def getLinkValues[A](linkColumn: LinkColumn[A], rowId: RowId): Future[Seq[JsonObject]] = {
    val tableId = linkColumn.table.id

    val toTableId = linkColumn.to.table.id
    val toColumnId = linkColumn.to.id

    val linkId = linkColumn.linkInformation._1
    val id1 = linkColumn.linkInformation._2
    val id2 = linkColumn.linkInformation._3

    for {
      t <- connection.begin()

      (t, result) <- linkColumn.to match {
        case c: MultiLanguageColumn[_] => t.query(
          s"""
             |SELECT to_table.id, json_object_agg(DISTINCT COALESCE(langtag, 'de_DE'), to_table_lang.column_$toColumnId) AS column_$toColumnId
             |FROM user_table_$tableId AS from_table
             |JOIN link_table_$linkId
             |  ON from_table.id = link_table_$linkId.$id1
             |JOIN user_table_$toTableId AS to_table
             |  ON to_table.id = link_table_$linkId.$id2
             |LEFT JOIN user_table_lang_$toTableId AS to_table_lang
             |  ON to_table.id = to_table_lang.id
             |WHERE from_table.id = ?
             |GROUP BY to_table.id""".stripMargin, Json.arr(rowId))
        case c: SimpleValueColumn[_] => t.query(
          s"""
             |SELECT to_table.id, to_table.column_$toColumnId
             |FROM user_table_$tableId AS from_table
             |JOIN link_table_$linkId
             |  ON from_table.id = link_table_$linkId.$id1
             |JOIN user_table_$toTableId AS to_table
             |  ON to_table.id = link_table_$linkId.$id2
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
