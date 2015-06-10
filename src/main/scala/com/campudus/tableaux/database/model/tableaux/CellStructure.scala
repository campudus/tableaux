package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

class CellStructure(val connection: DatabaseConnection) extends DatabaseQuery {

  def update[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Unit] = {
    connection.singleQuery(s"UPDATE user_table_$tableId SET column_$columnId = ? WHERE id = ?", Json.arr(value, rowId))
  } map { _ => () }

  def updateLink(tableId: IdType, linkColumnId: IdType, leftRow: IdType, rightRow: IdType): Future[Unit] = for {
    t <- connection.begin()
    (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
    linkId <- Future.successful(selectNotNull(result).head.get[IdType](0))
    (t, _) <- t.query(s"INSERT INTO link_table_$linkId VALUES (?, ?)", Json.arr(leftRow, rightRow))
    _ <- t.commit()
  } yield ()

  def putLinks(tableId: IdType, linkColumnId: IdType, from: IdType, tos: Seq[IdType]): Future[Unit] = {
    val paramStr = tos.map(_ => "(?, ?)").mkString(", ")
    val params = tos.flatMap(List(from, _))
    connection.verticle.logger.info(s"params=${params.mkString(", ")}")

    for {
      t <- connection.begin()
      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))
      linkId <- Future.successful(selectNotNull(result).head.get[IdType](0))
      (t, _) <- t.query(s"DELETE FROM link_table_$linkId", Json.arr())
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

  def getValue(tableId: IdType, columnId: IdType, rowId: IdType): Future[Any] = {
    connection.singleQuery(s"SELECT column_$columnId FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
  } map { selectNotNull(_).head.get[Any](0) }

  def getLinkValues(tableId: IdType, linkColumnId: IdType, rowId: IdType, toTableId: IdType, toColumnId: IdType): Future[Seq[JsonObject]] = {
    for {
      t <- connection.begin()

      (t, result) <- t.query("SELECT link_id FROM system_columns WHERE table_id = ? AND column_id = ?", Json.arr(tableId, linkColumnId))

      linkId <- Future.successful(selectNotNull(result).head.get[IdType](0))

      (t, result) <- t.query("SELECT table_id_1, table_id_2, column_id_1, column_id_2 FROM system_link_table WHERE link_id = ?", Json.arr(linkId))

      (id1, id2) <- Future.successful {
        val res = selectNotNull(result).head
        val linkTo2 = (res.get[IdType](1), res.get[IdType](3))

        if (linkTo2 == (toTableId, toColumnId)) {
          ("id_1", "id_2")
        } else {
          ("id_2", "id_1")
        }
      }

      (t, result) <- t.query(s"""
        |SELECT user_table_$toTableId.id, user_table_$toTableId.column_$toColumnId FROM user_table_$tableId
        |  JOIN link_table_$linkId
        |    ON user_table_$tableId.id = link_table_$linkId.$id1
        |  JOIN user_table_$toTableId
        |    ON user_table_$toTableId.id = link_table_$linkId.$id2
        |WHERE user_table_$tableId.id = ?""".stripMargin, Json.arr(rowId))

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
