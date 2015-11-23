package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

class RowModel(val connection: DatabaseConnection) extends DatabaseQuery {

  def createEmpty(tableId: TableId): Future[RowId] = {
    for {
      result <- connection.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id")
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  def createFull(tableId: TableId, values: Seq[(ColumnType[_], _)]): Future[RowId] = {
    val placeholder = values.map(_ => "?").mkString(", ")
    val columns = values.map { case (column: ColumnType[_], _) => s"column_${column.id}" }.mkString(", ")
    val binds = values.map { case (_, value) => value }

    for {
      result <- connection.query(s"INSERT INTO user_table_$tableId ($columns) VALUES ($placeholder) RETURNING id", Json.arr(binds: _*))
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  def createTranslations(tableId: TableId, rowId: RowId, values: Seq[(ColumnType[_], Seq[(String, _)])]): Future[Unit] = {
    for {
      _ <- connection.transactionalFoldLeft(values) { (t, _, value) =>
        val column = s"column_${value._1.id}"
        val translations = value._2

        val placeholder = translations.map(_ => "(?, ?, ?)").mkString(", ")
        val binds = translations.flatMap(translation => Seq(rowId, translation._1, translation._2))

        t.query(s"INSERT INTO user_table_lang_$tableId (id, langtag, $column) VALUES $placeholder", Json.arr(binds: _*))
      }
    } yield ()
  }

  def retrieve(tableId: TableId, rowId: RowId, columns: Seq[ColumnType[_]]): Future[(RowId, Seq[AnyRef])] = {
    val projection = generateProjection(columns)
    val fromClause = generateFromClause(tableId, columns)

    for {
      result <- connection.query(s"SELECT $projection FROM $fromClause WHERE ut.id = ? GROUP BY ut.id", Json.arr(rowId))
    } yield {
      val row = jsonArrayToSeq(selectNotNull(result).head)
      (row.head, mapResultRow(columns, row.drop(1)))
    }
  }

  def retrieveAll(tableId: TableId, columns: Seq[ColumnType[_]], pagination: Pagination): Future[Seq[(RowId, Seq[AnyRef])]] = {
    val projection = generateProjection(columns)
    val fromClause = generateFromClause(tableId, columns)

    for {
      result <- connection.query(s"SELECT $projection FROM $fromClause GROUP BY ut.id ORDER BY ut.id $pagination")
    } yield {
      getSeqOfJsonArray(result).map(jsonArrayToSeq).map { row =>
        (row.head, mapResultRow(columns, row.drop(1)))
      }
    }
  }

  def size(tableId: TableId): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM user_table_$tableId"

    connection.selectSingleValue(select)
  }

  def delete(tableId: TableId, rowId: RowId): Future[Unit] = {
    for {
      result <- connection.query(s"DELETE FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
    } yield {
      deleteNotNull(result)
    }
  }

  private def mapResultRow(columns: Seq[ColumnType[_]], result: Seq[AnyRef]): Seq[AnyRef] = {
    (columns, result).zipped map { (column: ColumnType[_], value: AnyRef) =>
      column match {
        case _: MultiLanguageColumn[_] =>
          if (value == null)
            Json.emptyObj()
          else
            Json.fromObjectString(value.toString)

        case _: LinkColumn[_] =>
          if (value == null)
            Json.emptyArr()
          else
            Json.fromArrayString(value.toString)

        case _ => value
      }
    }
  }

  private def generateFromClause(tableId: TableId, columns: Seq[ColumnType[_]]): String = {
    s"user_table_$tableId ut LEFT JOIN user_table_lang_$tableId utl ON (ut.id = utl.id)"
  }

  private def generateProjection(columns: Seq[ColumnType[_]]): String = {
    // TODO support for multi-language date & date-time

    val projection = columns map {
      case c: MultiLanguageColumn[_] =>
        s"CASE WHEN COUNT(utl.id) = 0 THEN NULL ELSE json_object_agg(DISTINCT COALESCE(utl.langtag, 'IGNORE'), utl.column_${c.id}) FILTER (WHERE utl.column_${c.id} IS NOT NULL) END AS column_${c.id}"
      case c: DateTimeColumn =>
        s"""TO_CHAR(ut.column_${c.id} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS column_${c.id}"""
      case c: DateColumn =>
        s"TO_CHAR(ut.column_${c.id}, 'YYYY-MM-DD') AS column_${c.id}"
      case c: SimpleValueColumn[_] =>
        s"ut.column_${c.id}"
      case c: LinkColumn[_] =>
        val linkId = c.linkInformation._1
        val id1 = c.linkInformation._2
        val id2 = c.linkInformation._3
        val toTableId = c.to.table.id

        val column = c.to match {
          case _: MultiLanguageColumn[_] =>
            s"json_build_object('id', ut${toTableId}.id, 'value', json_object_agg(utl${toTableId}.langtag, utl${toTableId}.column_${c.to.id})) AS column_${c.to.id}"
          case _: DateTimeColumn =>
            s"""TO_CHAR(ut${toTableId}.column_${c.id} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS column_${c.id}"""
          case _: DateColumn =>
            s"TO_CHAR(ut${toTableId}.column_${c.id}, 'YYYY-MM-DD') AS column_${c.id}"
          case _: SimpleValueColumn[_] =>
            s"json_build_object('id', ut${toTableId}.id, 'value', ut${toTableId}.column_${c.to.id}) AS column_${c.to.id}"
        }

        s"""(
            |SELECT
            | json_agg(sub.column_${c.to.id})
            |FROM
            |(
            | SELECT
            |   lt${linkId}.${id1} AS ${id1},
            |   $column
            | FROM
            |   link_table_${linkId} lt${linkId} JOIN
            |   user_table_${toTableId} ut${toTableId} ON (lt${linkId}.${id2} = ut${toTableId}.id)
            |   LEFT JOIN user_table_lang_${toTableId} utl${toTableId} ON (ut${toTableId}.id = utl${toTableId}.id)
            | WHERE column_${c.to.id} IS NOT NULL
            | GROUP BY ut${toTableId}.id, lt${linkId}.${id1}
            | ORDER BY ut${toTableId}.id
            |) sub
            |WHERE sub.${id1} = ut.id
            |GROUP BY sub.${id1}
            |) AS column_${c.id}
           """.stripMargin
      case _ => "NULL"
    }

    if (projection.nonEmpty) {
      s"ut.id,\n${projection.mkString(",\n")}"
    } else {
      s"ut.id"
    }
  }
}
