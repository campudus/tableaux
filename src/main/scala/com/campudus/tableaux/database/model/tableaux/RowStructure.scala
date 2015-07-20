package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.domain.{SimpleValueColumn, ColumnType, MultiLanguageColumn}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

class RowStructure(val connection: DatabaseConnection) extends DatabaseQuery {

  def create(tableId: TableId): Future[TableId] = {
    connection.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id")
  } map { insertNotNull(_).head.get[TableId](0) }

  def createFull(tableId: TableId, values: Seq[(ColumnId, _)]): Future[RowId] = {
    val qm = values.foldLeft(Seq[String]())((s, _) => s :+ "?").mkString(", ")
    val columns = values.foldLeft(Seq[String]())((s, tup) => s :+ s"column_${tup._1}").mkString(", ")
    val v = values.foldLeft(Seq[Any]())((s, tup) => s :+ tup._2)
    connection.query(s"INSERT INTO user_table_$tableId ($columns) VALUES ($qm) RETURNING id", Json.arr(v: _*))
  } map { insertNotNull(_).head.get[RowId](0) }

  def get(tableId: TableId, rowId: RowId, columns: Seq[ColumnType[_]]): Future[(RowId, Seq[AnyRef])] = {
    val projection = generateProjection(columns)
    val fromClause = generateFromClause(tableId)

    val result = connection.query(s"SELECT $projection FROM $fromClause WHERE ut.id = ? GROUP BY ut.id", Json.arr(rowId))

    result map { x =>
      val seq = jsonArrayToSeq(selectNotNull(x).head)
      (seq.head, mapResultRow(columns, seq.drop(1)))
    }
  }

  private def mapResultRow(columns: Seq[ColumnType[_]], result: Seq[AnyRef]): Seq[AnyRef] = {
    (columns, result).zipped map { (column: ColumnType[_], value: AnyRef) =>
      column match {
        case _: MultiLanguageColumn[_] => Json.fromObjectString(value.toString)
        case _ => value
      }
    }
  }

  private def generateFromClause(tableId: TableId): String = {
    s"user_table_$tableId ut LEFT JOIN user_table_lang_$tableId utl ON (ut.id = utl.id)"
  }

  private def generateProjection(columns: Seq[ColumnType[_]]): String = {
    val projection = columns map {
      case s: SimpleValueColumn[_] => s"column_${s.id}"
      case m: MultiLanguageColumn[_] => s"json_object_agg(DISTINCT COALESCE(utl.langtag, 'de_DE'), column_${m.id}) AS column_${m.id}"
      case _ => "NULL"
    }

    if (projection.nonEmpty) {
      s"ut.id,${projection.mkString(",")}"
    } else {
      s"ut.id"
    }
  }

  def getAll(tableId: TableId, columns: Seq[ColumnType[_]]): Future[Seq[(RowId, Seq[AnyRef])]] = {
    val projection = generateProjection(columns)
    val fromClause = generateFromClause(tableId)

    val result = connection.query(s"SELECT $projection FROM $fromClause GROUP BY ut.id ORDER BY ut.id")

    result map { x =>
      val seq = getSeqOfJsonArray(x) map { jsonArrayToSeq }
      seq map { s => (s.head, mapResultRow(columns, s.drop(1))) }
    }
  }

  def delete(tableId: TableId, rowId: RowId): Future[Unit] = {
    connection.query(s"DELETE FROM user_table_$tableId  WHERE id = ?", Json.arr(rowId))
  } map { deleteNotNull(_) }
}
