package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.domain.{SimpleValueColumn, ColumnType}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

class RowStructure(val connection: DatabaseConnection) extends DatabaseQuery {

  def create(tableId: IdType): Future[IdType] = {
    connection.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id")
  } map { insertNotNull(_).head.get[IdType](0) }

  def createFull(tableId: IdType, values: Seq[(IdType, _)]): Future[IdType] = {
    val qm = values.foldLeft(Seq[String]())((s, _) => s :+ "?").mkString(", ")
    val columns = values.foldLeft(Seq[String]())((s, tup) => s :+ s"column_${tup._1}").mkString(", ")
    val v = values.foldLeft(Seq[Any]())((s, tup) => s :+ tup._2)
    connection.query(s"INSERT INTO user_table_$tableId ($columns) VALUES ($qm) RETURNING id", Json.arr(v: _*))
  } map { insertNotNull(_).head.get[IdType](0) }

  def get(tableId: IdType, rowId: IdType, columns: Seq[ColumnType[_]]): Future[(IdType, Seq[AnyRef])] = {
    val projectionStr = generateProjection(columns)
    val result = connection.query(s"SELECT $projectionStr FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))

    result map { x =>
      val seq = jsonArrayToSeq(selectNotNull(x).head)
      (seq.head, seq.drop(1))
    }
  }

  private def generateProjection(columns: Seq[ColumnType[_]]): String = {
    val projection = columns map {
      case c: SimpleValueColumn[_] => s"column_${c.id}"
      case _ => "NULL"
    }

    if (projection.nonEmpty) {
      s"id, ${projection.mkString(",")}"
    } else {
      s"id"
    }
  }

  def getAll(tableId: IdType, columns: Seq[ColumnType[_]]): Future[Seq[(IdType, Seq[AnyRef])]] = {
    val projectionStr = generateProjection(columns)
    val result = connection.query(s"SELECT $projectionStr FROM user_table_$tableId ORDER BY id")

    result map { x =>
      val seq = getSeqOfJsonArray(x) map { jsonArrayToSeq }
      seq map { s => (s.head, s.drop(1)) }
    }
  }

  def delete(tableId: IdType, rowId: IdType): Future[Unit] = {
    connection.query(s"DELETE FROM user_table_$tableId  WHERE id = ?", Json.arr(rowId))
  } map { deleteNotNull(_) }
}
