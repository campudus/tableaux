package com.campudus.tableaux.database.model.tableaux

import java.util.UUID

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{MultiLanguageColumn, _}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.{Attachment, AttachmentFile, AttachmentModel}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{Json, _}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class UpdateRowModel(val connection: DatabaseConnection) extends DatabaseQuery {
  val attachmentModel = AttachmentModel(connection)

  def deleteRow(tableId: TableId, rowId: RowId): Future[Unit] = {
    for {
      result <- connection.query(s"DELETE FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
    } yield {
      deleteNotNull(result)
    }
  }

  def clearRow(table: Table, rowId: RowId, values: Seq[(ColumnType[_])]): Future[Unit] = {
    val (simple, multis, links, attachments) = ColumnType.splitIntoTypes(values)

    for {
      _ <- if (simple.isEmpty) Future.successful(()) else updateRow(table, rowId, simple.map((_, null)))
      _ <- if (multis.isEmpty) Future.successful(()) else clearTranslation(table, rowId, multis)
      _ <- if (links.isEmpty) Future.successful(()) else clearLinks(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else clearAttachments(table, rowId, attachments)
    } yield ()
  }

  private def clearTranslation(table: Table, rowId: RowId, columns: Seq[SimpleValueColumn[_]]): Future[_] = {
    val setExpression = columns.map({
      case MultiLanguageColumn(column) => s"column_${column.id} = NULL"
    }).mkString(", ")

    for {
      update <- connection.query(s"UPDATE user_table_lang_${table.id} SET $setExpression WHERE id = ?", Json.arr(rowId))
      _ = updateNotNull(update)
    } yield ()
  }

  private def clearLinks(table: Table, rowId: RowId, columns: Seq[LinkColumn]): Future[_] = {
    val futureSequence = columns.map({
      case column: LinkColumn =>
        val linkId = column.linkId
        val direction = column.linkDirection

        for {
          _ <- connection.query(s"DELETE FROM link_table_$linkId WHERE ${direction.fromSql} = ?", Json.arr(rowId))
        } yield ()
    })

    Future.sequence(futureSequence)
  }

  private def clearAttachments(table: Table, rowId: RowId, columns: Seq[AttachmentColumn]): Future[_] = {
    val cleared = columns.map((c: AttachmentColumn) => attachmentModel.deleteAll(table.id, c.id, rowId))
    Future.sequence(cleared)
  }

  def deleteLink(table: Table, column: LinkColumn, rowId: RowId, toRowId: RowId): Future[Unit] = {
    val rowIdColumn = column.linkDirection.fromSql
    val toIdColumn = column.linkDirection.toSql
    val linkTable = s"link_table_${column.linkId}"

    val sql = s"DELETE FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?"

    for {
      _ <- connection.query(sql, Json.arr(rowId, toRowId))
    } yield ()
  }

  def updateLinkOrder(table: Table, column: LinkColumn, rowId: RowId, toId: RowId, locationType: LocationType): Future[Unit] = {
    val rowIdColumn = column.linkDirection.fromSql
    val toIdColumn = column.linkDirection.toSql
    val orderColumn = column.linkDirection.orderingSql
    val linkTable = s"link_table_${column.linkId}"

    val listOfStatements: List[(String, JsonArray)] = locationType match {
      case LocationStart => List(
        (s"UPDATE $linkTable SET $orderColumn = (SELECT MIN($orderColumn) - 1 FROM $linkTable WHERE $rowIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ? AND $orderColumn > (SELECT MIN($orderColumn) FROM $linkTable WHERE $rowIdColumn = ?)", Json.arr(rowId, rowId, toId, rowId))
      )
      case LocationEnd => List(
        (s"UPDATE $linkTable SET $orderColumn = (SELECT MAX($orderColumn) + 1 FROM $linkTable WHERE $rowIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ? AND $orderColumn < (SELECT MAX($orderColumn) FROM $linkTable WHERE $rowIdColumn = ?)", Json.arr(rowId, rowId, toId, rowId))
      )
      case LocationBefore(relativeTo) => List(
        (s"UPDATE $linkTable SET $orderColumn = (SELECT $orderColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ?", Json.arr(rowId, relativeTo, rowId, toId)),
        (s"UPDATE $linkTable SET $orderColumn = $orderColumn + 1 WHERE ($orderColumn >= (SELECT $orderColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?) AND ($rowIdColumn = ? AND $toIdColumn != ?))", Json.arr(rowId, relativeTo, rowId, toId))
      )
    }

    val normalize = (
      s"""
         |UPDATE $linkTable
         |SET $orderColumn = r.ordering
         |FROM (SELECT $rowIdColumn, $toIdColumn, row_number() OVER (ORDER BY $rowIdColumn, $orderColumn) AS ordering FROM $linkTable WHERE $rowIdColumn = ?) r
         |WHERE $linkTable.$rowIdColumn = r.$rowIdColumn AND $linkTable.$toIdColumn = r.$toIdColumn
       """.stripMargin, Json.arr(rowId))

    for {
      t <- connection.begin()

      // Check if row exists
      (t, result) <- t.query(s"SELECT id FROM user_table_${table.id} WHERE id = ?", Json.arr(rowId))
      _ = selectNotNull(result)

      // Check if row exists
      (t, result) <- t.query(s"SELECT id FROM user_table_${column.to.table.id} WHERE id = ?", Json.arr(toId))
      _ = selectNotNull(result)

      t <- locationType match {
        case LocationBefore(relativeTo) =>

          // Check if row is linked
          t.query(s"SELECT $toIdColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?", Json.arr(rowId, relativeTo))
            .map({
              case (t, result) =>
                selectNotNull(result)
                t
            })

        case _ =>
          Future.successful(t)
      }

      (t, results) <- (listOfStatements :+ normalize).foldLeft(Future.successful((t, Vector[JsonObject]()))) {
        case (fTuple, (query, bindParams)) =>
          for {
            (latestTransaction, results) <- fTuple
            (lastT, result) <- latestTransaction.query(query, bindParams)
          } yield (lastT, results :+ result)
      }

      _ <- t.commit()
    } yield ()
  }

  def updateRow(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simple, multis, links, attachments)) =>
        for {
          _ <- if (simple.isEmpty) Future.successful(()) else updateSimple(table, rowId, simple)
          _ <- if (multis.isEmpty) Future.successful(()) else updateTranslations(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else updateLinks(table, rowId, links)
          _ <- if (attachments.isEmpty) Future.successful(()) else updateAttachments(table, rowId, attachments)
        } yield ()
    }
  }

  private def updateSimple(table: Table, rowId: RowId, simple: List[(SimpleValueColumn[_], Option[_])]): Future[Unit] = {
    val setExpression = simple.map({
      case (column: SimpleValueColumn[_], _) => s"column_${column.id} = ?"
    }).mkString(", ")

    val binds = simple.map(_._2.getOrElse(null)) ++ List(rowId)

    for {
      update <- connection.query(s"UPDATE user_table_${table.id} SET $setExpression WHERE id = ?", Json.arr(binds: _*))
      _ = updateNotNull(update)
    } yield ()
  }

  private def updateTranslations(table: Table, rowId: RowId, values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]): Future[_] = {
    val entries = for {
      (column, langs) <- values
      (langTag: String, value) <- langs
    } yield (langTag, (column, value))

    val columnsForLang = entries.groupBy(_._1).mapValues(_.map(_._2))

    if (columnsForLang.nonEmpty) {
      connection.transactionalFoldLeft(columnsForLang.toSeq) { (t, _, langValues) =>
        val langtag = langValues._1
        val values = langValues._2

        val setExpression = values.map({
          case (column, _) => s"column_${column.id} = ?"
        }).mkString(", ")

        val binds = values.map(_._2.orNull).toList ++ List(rowId, langtag)

        for {
          (t, _) <- t.query(s"INSERT INTO user_table_lang_${table.id}(id, langtag) SELECT ?, ? WHERE NOT EXISTS (SELECT id FROM user_table_lang_${table.id} WHERE id = ? AND langtag = ?)", Json.arr(rowId, langtag, rowId, langtag))
          (t, result) <- t.query(s"UPDATE user_table_lang_${table.id} SET $setExpression WHERE id = ? AND langtag = ?", Json.arr(binds: _*))
        } yield (t, result)
      }
    } else {
      Future.failed(new IllegalArgumentException("error.json.arguments"))
    }
  }

  private def updateLinks(table: Table, rowId: RowId, values: Seq[(LinkColumn, Seq[RowId])]): Future[Unit] = {
    val futureSequence = values.map({
      case (column, toIds) =>
        val linkId = column.linkId
        val direction = column.linkDirection

        val union = toIds.map(_ => s"SELECT ?, ?, nextval('link_table_1_${direction.orderingSql}_seq') WHERE NOT EXISTS (SELECT ${direction.fromSql}, ${direction.toSql} FROM link_table_$linkId WHERE ${direction.fromSql} = ? AND ${direction.toSql} = ?)").mkString(" UNION ")
        val binds = toIds.flatMap(to => List(rowId, to, rowId, to))

        for {
          _ <- {
            if (toIds.nonEmpty) {
              connection.query(s"INSERT INTO link_table_$linkId(${direction.fromSql}, ${direction.toSql}, ${direction.orderingSql}) $union", Json.arr(binds: _*))
            } else {
              Future.successful(Json.emptyObj())
            }
          }
        } yield ()
    })

    Future.sequence(futureSequence).map(_ => ())
  }

  private def updateAttachments(table: Table, rowId: RowId, values: Seq[(AttachmentColumn, Seq[(UUID, Option[Ordering])])]): Future[Seq[Seq[AttachmentFile]]] = {
    val futureSequence = values.map({
      case (column, attachments) =>
        for {
          currentAttachments <- attachmentModel.retrieveAll(table.id, column.id, rowId)
          attachmentFiles <- attachments.foldLeft(Future.successful(List[AttachmentFile]())) {
            case (future, (uuid, ordering)) =>
              for {
                list <- future
                addedOrUpdatedAttachment <- if (currentAttachments.exists(_.file.file.uuid.equals(uuid))) {
                  attachmentModel.update(Attachment(table.id, column.id, rowId, uuid, ordering))
                } else {
                  attachmentModel.add(Attachment(table.id, column.id, rowId, uuid, ordering))
                }
              } yield addedOrUpdatedAttachment :: list
          }
        } yield attachmentFiles
    })

    Future.sequence(futureSequence)
  }
}

class CreateRowModel(val connection: DatabaseConnection) extends DatabaseQuery {
  val attachmentModel = AttachmentModel(connection)

  def createRow(tableId: TableId, values: Seq[(ColumnType[_], _)]): Future[RowId] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simple, multis, links, attachments)) =>
        for {
          rowId <- if (simple.isEmpty) createEmpty(tableId) else createSimple(tableId, simple)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslations(tableId, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(tableId, rowId, links)
          _ <- if (attachments.isEmpty) Future.successful(()) else createAttachments(tableId, rowId, attachments)
        } yield rowId
    }
  }

  private def createEmpty(tableId: TableId): Future[RowId] = {
    for {
      result <- connection.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id")
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  private def createSimple(tableId: TableId, values: Seq[(SimpleValueColumn[_], Option[_])]): Future[RowId] = {
    val placeholder = values.map(_ => "?").mkString(", ")
    val columns = values.map { case (column: ColumnType[_], _) => s"column_${column.id}" }.mkString(", ")
    val binds = values.map { case (_, value) => value.getOrElse(null) }

    for {
      result <- connection.query(s"INSERT INTO user_table_$tableId ($columns) VALUES ($placeholder) RETURNING id", Json.arr(binds: _*))
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  private def createTranslations(tableId: TableId, rowId: RowId, values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]): Future[_] = {
    val entries = for {
      (column, langs) <- values
      (langTag: String, value) <- langs
    } yield (langTag, (column, value))

    val columnsForLang = entries.groupBy(_._1).mapValues(_.map(_._2))

    if (columnsForLang.nonEmpty) {
      connection.transactionalFoldLeft(columnsForLang.toSeq) { (t, _, langValues) =>
        val columns = langValues._2.map { case (col, _) => s"column_${col.id}" }.mkString(", ")
        val placeholder = langValues._2.map(_ => "?").mkString(", ")
        val insertStmt = s"INSERT INTO user_table_lang_$tableId (id, langtag, $columns) VALUES (?, ?, $placeholder)"
        val insertBinds = List(rowId, langValues._1) ::: langValues._2.map(_._2.orNull).toList

        for {
          (t, result) <- t.query(insertStmt, Json.arr(insertBinds: _*))
        } yield (t, result)
      }
    } else {
      // No values put into multilanguage columns, should be okay
      Future.successful(())
    }
  }

  private def createLinks(tableId: TableId, rowId: RowId, values: Seq[(LinkColumn, Seq[RowId])]): Future[_] = {
    val futureSequence = values.map({
      case (column: LinkColumn, toIds) =>
        val linkId = column.linkId
        val direction = column.linkDirection

        val union = toIds.map(_ => s"SELECT ?, ?, nextval('link_table_1_${direction.orderingSql}_seq') WHERE NOT EXISTS (SELECT ${direction.fromSql}, ${direction.toSql} FROM link_table_$linkId WHERE ${direction.fromSql} = ? AND ${direction.toSql} = ?)").mkString(" UNION ")
        val binds = toIds.flatMap(to => List(rowId, to, rowId, to))

        for {
          t <- connection.begin()

          (t, _) <- t.query(s"DELETE FROM link_table_$linkId WHERE ${direction.fromSql} = ?", Json.arr(rowId))

          (t, _) <- {
            if (toIds.nonEmpty) {
              t.query(s"INSERT INTO link_table_$linkId(${direction.fromSql}, ${direction.toSql}, ${direction.orderingSql}) $union", Json.arr(binds: _*))
            } else {
              Future.successful((t, Json.emptyObj()))
            }
          }

          _ <- t.commit()
        } yield ()
    })

    Future.sequence(futureSequence)
  }

  private def createAttachments(tableId: TableId, rowId: RowId, values: Seq[(AttachmentColumn, Seq[(UUID, Option[Ordering])])]): Future[_] = {
    val futureSequence = for {
      (column: AttachmentColumn, attachmentValue) <- values
      attachments = attachmentValue.map({
        case (uuid, ordering) =>
          Attachment(tableId, column.id, rowId, uuid, ordering)
      })
    } yield {
      attachmentModel.replace(tableId, column.id, rowId, attachments)
    }

    Future.sequence(futureSequence)
  }
}

class RetrieveRowModel(val connection: DatabaseConnection) extends DatabaseQuery {

  private val dateTimeFormat = "YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\""
  private val dateFormat = "YYYY-MM-DD"

  def retrieve(tableId: TableId, rowId: RowId, columns: Seq[ColumnType[_]]): Future[(RowId, Seq[Any])] = {
    val projection = generateProjection(columns)
    val fromClause = generateFromClause(tableId)

    for {
      result <- connection.query(s"SELECT $projection FROM $fromClause WHERE ut.id = ? GROUP BY ut.id", Json.arr(rowId))
    } yield {
      val row = jsonArrayToSeq(selectNotNull(result).head)
      (rowId, mapResultRow(columns, row.drop(1)))
    }
  }

  def retrieveAll(tableId: TableId, columns: Seq[ColumnType[_]], pagination: Pagination): Future[Seq[(RowId, Seq[Any])]] = {
    val projection = generateProjection(columns)
    val fromClause = generateFromClause(tableId)

    for {
      result <- connection.query(s"SELECT $projection FROM $fromClause GROUP BY ut.id ORDER BY ut.id $pagination")
    } yield {
      resultObjectToJsonArray(result).map(jsonArrayToSeq).map {
        row =>
          (row.head.asInstanceOf[RowId], mapResultRow(columns, row.drop(1)))
      }
    }
  }

  def size(tableId: TableId): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM user_table_$tableId"

    connection.selectSingleValue(select)
  }

  private def mapValueByColumnType(column: ColumnType[_], value: Any): Any = {
    column match {
      case MultiLanguageColumn(_) =>
        if (value == null)
          Json.emptyObj()
        else
          Json.fromObjectString(value.toString)

      case _: LinkColumn =>
        if (value == null)
          Json.emptyArr()
        else
          Json.fromArrayString(value.toString)

      case _ =>
        value
    }
  }

  private def mapResultRow(columns: Seq[ColumnType[_]], result: Seq[Any]): Seq[Any] = {
    val (concatColumnOpt, zipped) = columns.headOption match {
      case Some(c: ConcatColumn) => (Some(c), (columns.drop(1), result.drop(1)).zipped)
      case _ => (None, (columns, result).zipped)
    }

    concatColumnOpt match {
      case None => zipped.map(mapValueByColumnType)
      case Some(concatColumn) =>
        val identifierColumns = zipped.filter({ (column: ColumnType[_], _) =>
          concatColumn.columns.exists(_.id == column.id)
        }).zipped

        import scala.collection.JavaConverters._

        val joinedValue = identifierColumns.foldLeft(List.empty[Any])({
          (joined, columnValue) =>
            joined.:+(mapValueByColumnType(columnValue._1, columnValue._2))
        }).asJava

        (columns.drop(1), result.drop(1)).zipped.map(mapValueByColumnType).+:(joinedValue)
    }
  }

  private def generateFromClause(tableId: TableId): String = {
    s"user_table_$tableId ut LEFT JOIN user_table_lang_$tableId utl ON (ut.id = utl.id)"
  }

  private def parseDateTimeSql(column: String): String = {
    s"TO_CHAR($column AT TIME ZONE 'UTC', '$dateTimeFormat')"
  }

  private def parseDateSql(column: String): String = {
    s"TO_CHAR($column, '$dateFormat')"
  }

  private def generateProjection(columns: Seq[ColumnType[_]]): String = {
    val projection = columns map {
      case _: ConcatColumn | _: AttachmentColumn =>
        // Values will be calculated/fetched after select
        // See TableauxModel.mapRawRows
        "NULL"

      case MultiLanguageColumn(c) =>
        val column = c match {
          case _: DateTimeColumn =>
            parseDateTimeSql(s"utl.column_${c.id}")
          case _: DateColumn =>
            parseDateSql(s"utl.column_${c.id}")
          case _ =>
            s"utl.column_${c.id}"
        }

        s"CASE WHEN COUNT(utl.id) = 0 THEN NULL ELSE json_object_agg(DISTINCT COALESCE(utl.langtag, 'IGNORE'), $column) FILTER (WHERE utl.column_${c.id} IS NOT NULL) END AS column_${c.id}"

      case c: DateTimeColumn =>
        parseDateTimeSql(s"ut.column_${c.id}") + s" AS column_${c.id}"

      case c: DateColumn =>
        parseDateSql(s"ut.column_${c.id}") + s" AS column_${c.id}"

      case c: SimpleValueColumn[_] =>
        s"ut.column_${c.id}"

      case c: LinkColumn =>
        val linkId = c.linkId
        val direction = c.linkDirection
        val toTableId = c.to.table.id

        val (column, value) = c.to match {
          case _: ConcatColumn =>
            // Values will be calculated/fetched after select
            // See TableauxModel.mapRawRows
            (s"''", "NULL")

          case MultiLanguageColumn(_) =>
            val linkedColumn = c.to match {
              case _: DateTimeColumn =>
                parseDateTimeSql(s"utl$toTableId.column_${c.to.id}")
              case _: DateColumn =>
                parseDateSql(s"utl$toTableId.column_${c.to.id}")
              case _ =>
                s"utl$toTableId.column_${c.to.id}"
            }

            // No distinct needed, just one result
            (s"column_${c.to.id}", s"json_object_agg(utl$toTableId.langtag, $linkedColumn)")

          case _: DateTimeColumn =>
            (s"column_${c.to.id}", parseDateTimeSql(s"ut$toTableId.column_${c.id}") + s" AS column_${c.id}")

          case _: DateColumn =>
            (s"column_${c.to.id}", parseDateSql(s"ut$toTableId.column_${c.id}"))

          case _: SimpleValueColumn[_] =>
            (s"column_${c.to.id}", s"ut$toTableId.column_${c.to.id}")
        }

        s"""(
            |SELECT
            | json_agg(sub.value)
            |FROM
            |(
            | SELECT
            |   lt$linkId.${direction.fromSql} AS ${direction.fromSql},
            |   json_build_object('id', ut$toTableId.id, 'value', $value) AS value
            | FROM
            |   link_table_$linkId lt$linkId
            |   JOIN user_table_$toTableId ut$toTableId ON (lt$linkId.${direction.toSql} = ut$toTableId.id)
            |   LEFT JOIN user_table_lang_$toTableId utl$toTableId ON (ut$toTableId.id = utl$toTableId.id)
            | WHERE $column IS NOT NULL
            | GROUP BY ut$toTableId.id, lt$linkId.${direction.fromSql}, lt$linkId.${direction.orderingSql}
            | ORDER BY lt$linkId.${direction.fromSql}, lt$linkId.${direction.orderingSql}
            |) sub
            |WHERE sub.${direction.fromSql} = ut.id
            |GROUP BY sub.${direction.fromSql}
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
