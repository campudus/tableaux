package com.campudus.tableaux.database.model.tableaux

import java.util.UUID

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{MultiLanguageColumn, RowLevelAnnotations, _}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.{Attachment, AttachmentFile, AttachmentModel}
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.{RowNotFoundException, UnknownServerException, UnprocessableEntityException}
import org.joda.time.DateTime
import org.vertx.scala.core.json.{Json, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private object ModelHelper {

  val dateTimeFormat = "YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\""
  val dateFormat = "YYYY-MM-DD"

  def parseDateTimeSql(column: String): String = {
    s"TO_CHAR($column AT TIME ZONE 'UTC', '$dateTimeFormat')"
  }

  def parseDateSql(column: String): String = {
    s"TO_CHAR($column, '$dateFormat')"
  }
}

sealed trait UpdateCreateRowModelHelper {
  protected[this] val connection: DatabaseConnection

  def rowExists(t: connection.Transaction, tableId: TableId, rowId: RowId)(
      implicit ec: ExecutionContext): Future[connection.Transaction] = {
    t.selectSingleValue[Boolean](s"SELECT COUNT(*) = 1 FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
      .flatMap({
        case (t, true) => Future.successful(t)
        case (_, false) => Future.failed(RowNotFoundException(tableId, rowId))
      })
  }

  def updateLinks(table: Table, rowId: RowId, values: Seq[(LinkColumn, Seq[RowId])])(
      implicit ec: ExecutionContext): Future[Unit] = {
    val futureSequence = values.map({
      case (column: LinkColumn, toIds) =>
        val linkId = column.linkId
        val direction = column.linkDirection

        val union = toIds
          .map(_ => {
            s"""
               |SELECT ?, ?, nextval('link_table_${linkId}_${direction.orderingSql}_seq')
               |WHERE
               |NOT EXISTS (SELECT ${direction.fromSql}, ${direction.toSql} FROM link_table_$linkId WHERE ${direction.fromSql} = ? AND ${direction.toSql} = ?) AND
               |(SELECT COUNT(*) FROM link_table_$linkId WHERE ${direction.fromSql} = ?) + ? <= (SELECT ${direction.toCardinality} FROM system_link_table WHERE link_id = ?) AND
               |(SELECT COUNT(*) FROM link_table_$linkId WHERE ${direction.toSql} = ?) + 1 <= (SELECT ${direction.fromCardinality} FROM system_link_table WHERE link_id = ?)
               |""".stripMargin
          })
          .mkString(" UNION ")
        val binds = toIds.zipWithIndex
          .flatMap({
            case (to, index) => List(rowId, to, rowId, to, rowId, index.toLong + 1, linkId, to, linkId)
          })

        connection.transactional(t => {
          for {
            // check if row (where we want to add the links) really exists
            t <- rowExists(t, column.table.id, rowId)

            // check if "to-be-linked" rows really exist
            t <- toIds
              .foldLeft(Future(t))((futureT, toId) => {
                futureT.flatMap(t => rowExists(t, column.to.table.id, toId))
              })
              .recoverWith({
                case ex: Throwable => Future.failed(UnprocessableEntityException(ex.getMessage))
              })

            (t, _) <- if (toIds.nonEmpty) {
              t.query(
                  s"INSERT INTO link_table_$linkId(${direction.fromSql}, ${direction.toSql}, ${direction.orderingSql}) $union RETURNING *",
                  Json.arr(binds: _*)
                )
                .map({
                  // if size doesn't match we hit the cardinality limit
                  case (t, result) => (t, insertCheckSize(result, toIds.size))
                })
            } else {
              Future.successful((t, Json.emptyArr()))
            }
          } yield (t, ())
        })
    })

    Future.sequence(futureSequence).map(_ => ())
  }
}

class UpdateRowModel(val connection: DatabaseConnection) extends DatabaseQuery with UpdateCreateRowModelHelper {

  val attachmentModel = AttachmentModel(connection)

  def deleteRow(tableId: TableId, rowId: RowId): Future[Unit] = {
    for {
      result <- connection.query(s"DELETE FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
    } yield {
      deleteNotNull(result)
    }
  }

  def clearRow(
      table: Table,
      rowId: RowId,
      values: Seq[(ColumnType[_])],
      deleteRowFn: (Table, RowId) => Future[EmptyObject]
  ): Future[Unit] = {
    val (simple, multis, links, attachments) = ColumnType.splitIntoTypes(values)

    for {
      _ <- if (simple.isEmpty) Future.successful(()) else updateSimple(table, rowId, simple.map((_, None)))
      _ <- if (multis.isEmpty) Future.successful(()) else clearTranslation(table, rowId, multis)
      _ <- if (links.isEmpty) Future.successful(()) else clearLinks(table, rowId, links, deleteRowFn)
      _ <- if (attachments.isEmpty) Future.successful(()) else clearAttachments(table, rowId, attachments)
    } yield ()
  }

  private def clearTranslation(table: Table, rowId: RowId, columns: Seq[SimpleValueColumn[_]]): Future[_] = {
    val setExpression = columns
      .map({
        case MultiLanguageColumn(column) => s"column_${column.id} = NULL"
      })
      .mkString(", ")

    for {
      _ <- connection.query(s"UPDATE user_table_lang_${table.id} SET $setExpression WHERE id = ?", Json.arr(rowId))
    } yield ()
  }

  private def clearLinks(
      table: Table,
      rowId: RowId,
      columns: Seq[LinkColumn],
      deleteRowFn: (Table, RowId) => Future[EmptyObject]
  ): Future[_] = {
    val futureSequence = columns.map(column => {
      val fromIdColumn = column.linkDirection.fromSql

      val linkTable = s"link_table_${column.linkId}"

      for {
        _ <- if (column.linkDirection.constraint.deleteCascade) {
          deleteLinkedRows(table, rowId, column, deleteRowFn)
        } else {
          connection.query(s"DELETE FROM $linkTable WHERE $fromIdColumn = ?", Json.arr(rowId))
        }
      } yield ()
    })

    Future.sequence(futureSequence)
  }

  private def clearAttachments(table: Table, rowId: RowId, columns: Seq[AttachmentColumn]): Future[_] = {
    val cleared = columns.map((c: AttachmentColumn) => attachmentModel.deleteAll(table.id, c.id, rowId))
    Future.sequence(cleared)
  }

  private def deleteLinkedRows(
      table: Table,
      rowId: RowId,
      column: LinkColumn,
      deleteRowFn: (Table, RowId) => Future[EmptyObject]
  ): Future[_] = {
    val toIdColumn = column.linkDirection.toSql
    val fromIdColumn = column.linkDirection.fromSql
    val linkTable = s"link_table_${column.linkId}"

    val selectForeignRows =
      s"""|SELECT
          | lt.$toIdColumn
          |FROM
          | $linkTable lt
          |WHERE
          | lt.$fromIdColumn = ? AND
          | (SELECT COUNT(*) FROM $linkTable sub WHERE sub.$toIdColumn = lt.$toIdColumn) = 1;""".stripMargin

    // select foreign rows which are only used once
    // these which are only used once should be deleted then
    // do this in a recursive manner
    connection
      .query(selectForeignRows, Json.arr(rowId))
      .map(resultObjectToJsonArray)
      .map(_.map(_.getLong(0)))
      // TODO here we could end up in a endless loop,
      // TODO but we could argue that a schema like this doesn't make sense
      .flatMap(foreignRowIdSeq => {
        val futures = foreignRowIdSeq.map(deleteRowFn(column.to.table, _))

        Future.sequence(futures)
      })
  }

  def deleteLink(
      table: Table,
      column: LinkColumn,
      fromRowId: RowId,
      toRowId: RowId,
      deleteRowFn: (Table, RowId) => Future[EmptyObject]
  ): Future[Unit] = {
    val rowIdColumn = column.linkDirection.fromSql
    val toIdColumn = column.linkDirection.toSql
    val linkTable = s"link_table_${column.linkId}"

    val sql = s"DELETE FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?"

    for {
      _ <- if (column.linkDirection.constraint.deleteCascade) {
        deleteRowFn(column.to.table, toRowId)
      } else {
        connection.query(sql, Json.arr(fromRowId, toRowId))
      }
    } yield ()
  }

  def updateLinkOrder(
      table: Table,
      column: LinkColumn,
      rowId: RowId,
      toId: RowId,
      locationType: LocationType
  ): Future[Unit] = {
    val rowIdColumn = column.linkDirection.fromSql
    val toIdColumn = column.linkDirection.toSql
    val orderColumn = column.linkDirection.orderingSql
    val linkTable = s"link_table_${column.linkId}"

    val listOfStatements: List[(String, JsonArray)] = locationType match {
      case LocationStart =>
        List(
          (s"UPDATE $linkTable SET $orderColumn = (SELECT MIN($orderColumn) - 1 FROM $linkTable WHERE $rowIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ? AND $orderColumn > (SELECT MIN($orderColumn) FROM $linkTable WHERE $rowIdColumn = ?)",
           Json.arr(rowId, rowId, toId, rowId))
        )
      case LocationEnd =>
        List(
          (s"UPDATE $linkTable SET $orderColumn = (SELECT MAX($orderColumn) + 1 FROM $linkTable WHERE $rowIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ? AND $orderColumn < (SELECT MAX($orderColumn) FROM $linkTable WHERE $rowIdColumn = ?)",
           Json.arr(rowId, rowId, toId, rowId))
        )
      case LocationBefore(relativeTo) =>
        List(
          (s"UPDATE $linkTable SET $orderColumn = (SELECT $orderColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ?",
           Json.arr(rowId, relativeTo, rowId, toId)),
          (s"UPDATE $linkTable SET $orderColumn = $orderColumn + 1 WHERE ($orderColumn >= (SELECT $orderColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?) AND ($rowIdColumn = ? AND $toIdColumn != ?))",
           Json.arr(rowId, relativeTo, rowId, toId))
        )
    }

    val normalize = (s"""
                        |UPDATE $linkTable
                        |SET $orderColumn = r.ordering
                        |FROM (SELECT $rowIdColumn, $toIdColumn, row_number() OVER (ORDER BY $rowIdColumn, $orderColumn) AS ordering FROM $linkTable WHERE $rowIdColumn = ?) r
                        |WHERE $linkTable.$rowIdColumn = r.$rowIdColumn AND $linkTable.$toIdColumn = r.$toIdColumn
       """.stripMargin,
                     Json.arr(rowId))

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
          t.query(s"SELECT $toIdColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?",
                   Json.arr(rowId, relativeTo))
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

  private def updateSimple(
      table: Table,
      rowId: RowId,
      simple: List[(SimpleValueColumn[_], Option[Any])]
  ): Future[Unit] = {
    val setExpression = simple
      .map({
        case (column: SimpleValueColumn[_], _) => s"column_${column.id} = ?"
      })
      .mkString(", ")

    val binds = simple.map({
      case (_, valueOpt) => valueOpt.orNull
    }) ++ List(rowId)

    for {
      update <- connection.query(s"UPDATE user_table_${table.id} SET $setExpression WHERE id = ?", Json.arr(binds: _*))
      _ = updateNotNull(update)
    } yield ()
  }

  private def updateTranslations(
      table: Table,
      rowId: RowId,
      values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]
  ): Future[_] = {
    val entries = for {
      (column, langtagValueOptMap) <- values
      (langtag: String, valueOpt) <- langtagValueOptMap
    } yield (langtag, (column, valueOpt))

    val columnsForLang = entries
      .groupBy({ case (langtag, _) => langtag })
      .mapValues(_.map({ case (_, columnValueOpt) => columnValueOpt }))

    if (columnsForLang.nonEmpty) {
      connection.transactionalFoldLeft(columnsForLang.toSeq) {
        case (t, _, (langtag, columnValueOptSeq)) =>
          val setExpression = columnValueOptSeq
            .map({
              case (column, _) => s"column_${column.id} = ?"
            })
            .mkString(", ")

          val binds = columnValueOptSeq.map({ case (_, valueOpt) => valueOpt.orNull }).toList ++ List(rowId, langtag)

          for {
            (t, _) <- t.query(
              s"INSERT INTO user_table_lang_${table.id}(id, langtag) SELECT ?, ? WHERE NOT EXISTS (SELECT id FROM user_table_lang_${table.id} WHERE id = ? AND langtag = ?)",
              Json.arr(rowId, langtag, rowId, langtag)
            )
            (t, result) <- t.query(
              s"UPDATE user_table_lang_${table.id} SET $setExpression WHERE id = ? AND langtag = ?",
              Json.arr(binds: _*))
          } yield (t, result)
      }
    } else {
      Future.failed(new IllegalArgumentException("error.json.arguments"))
    }
  }

  private def updateAttachments(
      table: Table,
      rowId: RowId,
      values: Seq[(AttachmentColumn, Seq[(UUID, Option[Ordering])])]
  ): Future[Seq[Seq[AttachmentFile]]] = {
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

  def updateRowsAnnotations(tableId: TableId, finalFlagOpt: Option[Boolean]): Future[Unit] = {
    for {
      _ <- finalFlagOpt match {
        case None => Future.successful(())
        case Some(finalFlag) =>
          connection.query(s"UPDATE user_table_$tableId SET final = ?", Json.arr(finalFlag))
      }
    } yield ()
  }

  def updateRowAnnotations(tableId: TableId, rowId: RowId, finalFlagOpt: Option[Boolean]): Future[Unit] = {
    for {
      _ <- finalFlagOpt match {
        case None => Future.successful(())
        case Some(finalFlag) =>
          connection.query(s"UPDATE user_table_$tableId SET final = ? WHERE id = ?", Json.arr(finalFlag, rowId))
      }
    } yield ()
  }

  def addOrMergeCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  ): Future[(UUID, Seq[String], DateTime)] = {
    import ModelHelper._

    val tableId = column.table.id
    val newUuid = UUID.randomUUID()

    val insertStatement =
      s"""
         |INSERT INTO
         |  user_table_annotations_$tableId(row_id, column_id, uuid, langtags, type, value)
         |VALUES (?, ?, ?, ?::text[], ?, ?)
         |RETURNING ${parseDateTimeSql("created_at")}""".stripMargin

    val insertBinds = Json.arr(
      rowId,
      column.id,
      newUuid.toString,
      langtags.mkString("{", ",", "}"),
      annotationType.toString,
      value
    )

    // https://github.com/vert-x3/vertx-mysql-postgresql-client/pull/75
    // can't use Arrays here, need to transform array to json array
    val updateStatement =
      s"""
         |UPDATE
         |  user_table_annotations_$tableId
         |SET
         |  langtags = subquery.langtags
         |FROM (
         |  SELECT
         |    row_id,
         |    column_id,
         |    uuid,
         |    type,
         |    value,
         |    (
         |      SELECT array(
         |        SELECT DISTINCT unnest(array_cat(langtags, ?::text[])) AS langtag ORDER BY langtag
         |      )
         |    ) AS langtags
         |  FROM
         |    user_table_annotations_$tableId
         |  WHERE
         |    row_id = ? AND
         |    column_id = ? AND
         |    type = ? AND
         |    value = ?
         |) AS subquery
         |WHERE
         |  user_table_annotations_$tableId.row_id = subquery.row_id AND
         |  user_table_annotations_$tableId.column_id = subquery.column_id AND
         |  user_table_annotations_$tableId.type = subquery.type AND
         |  user_table_annotations_$tableId.value = subquery.value
         |RETURNING
         |  user_table_annotations_$tableId.uuid,
         |  array_to_json(user_table_annotations_$tableId.langtags),
         |  ${parseDateTimeSql(s"user_table_annotations_$tableId.created_at")}""".stripMargin

    val updateBinds = Json.arr(
      langtags.mkString("{", ",", "}"),
      rowId,
      column.id,
      annotationType.toString,
      value
    )

    connection.transactional(t => {
      for {
        (t, _) <- t.query(s"LOCK TABLE user_table_annotations_$tableId IN EXCLUSIVE MODE")

        // first, try to update possible existing annotations...
        (t, rawRow) <- t
          .query(updateStatement, updateBinds)
          .map({
            case (t, result) => (t, resultObjectToJsonArray(result).map(jsonArrayToSeq).headOption)
          })

        updateResult = rawRow match {
          case Some(Seq(uuidStr: String, langtagsStr: String, createdAt: String)) =>
            import scala.collection.JavaConverters._

            Some(
              UUID.fromString(uuidStr),
              Json.fromArrayString(langtagsStr).asScala.map(_.asInstanceOf[String]).toList,
              DateTime.parse(createdAt)
            )
          case _ => None
        }

        // second, check if there was already a annotation...
        // ... otherwise insert a new one
        (t, result) <- updateResult match {
          case Some(s) => Future.successful((t, s))
          case _ =>
            t.query(insertStatement, insertBinds)
              .map({
                case (t, result) => (t, DateTime.parse(insertNotNull(result).head.getString(0)))
              })
              .map({
                case (t, createdAt) => (t, (newUuid, langtags, createdAt))
              })
        }

        // and then return either existing/merged values or new inserted values
      } yield (t, result)
    })
  }

  def deleteCellAnnotation(column: ColumnType[_], rowId: RowId, uuid: UUID): Future[_] = {
    val delete =
      s"DELETE FROM user_table_annotations_${column.table.id} WHERE row_id = ? AND column_id = ? AND uuid = ?"
    val binds = Json.arr(rowId, column.id, uuid.toString)

    connection.query(delete, binds)
  }

  def deleteCellAnnotation(column: ColumnType[_], rowId: RowId, uuid: UUID, langtag: String): Future[_] = {
    val delete =
      s"UPDATE user_table_annotations_${column.table.id} SET langtags = ARRAY_REMOVE(langtags, ?) WHERE row_id = ? AND column_id = ? AND uuid = ?"
    val binds = Json.arr(langtag, rowId, column.id, uuid.toString)

    connection.query(delete, binds)
  }
}

class CreateRowModel(val connection: DatabaseConnection) extends DatabaseQuery with UpdateCreateRowModelHelper {
  val attachmentModel = AttachmentModel(connection)

  def createRow(table: Table, values: Seq[(ColumnType[_], _)]): Future[RowId] = {
    val tableId = table.id
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simple, multis, links, attachments)) =>
        for {
          rowId <- if (simple.isEmpty) createEmpty(tableId) else createSimple(tableId, simple)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslations(tableId, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else updateLinks(table, rowId, links)
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

  private def createSimple(tableId: TableId, values: Seq[(SimpleValueColumn[_], Option[Any])]): Future[RowId] = {
    val placeholder = values.map(_ => "?").mkString(", ")
    val columns = values.map({ case (column: ColumnType[_], _) => s"column_${column.id}" }).mkString(", ")
    val binds = values.map({ case (_, value) => value.orNull })

    for {
      result <- connection
        .query(s"INSERT INTO user_table_$tableId ($columns) VALUES ($placeholder) RETURNING id", Json.arr(binds: _*))
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  private def createTranslations(
      tableId: TableId,
      rowId: RowId,
      values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]
  ): Future[_] = {
    val entries = for {
      (column, langtagValueOptMap) <- values
      (langtag: String, valueOpt) <- langtagValueOptMap
    } yield (langtag, (column, valueOpt))

    val columnsForLang = entries
      .groupBy({ case (langtag, _) => langtag })
      .mapValues(_.map({ case (_, columnValueOpt) => columnValueOpt }))

    if (columnsForLang.nonEmpty) {
      connection.transactionalFoldLeft(columnsForLang.toSeq) {
        case (t, _, (langtag, columnValueOptSeq)) =>
          val columns = columnValueOptSeq.map({ case (column, _) => s"column_${column.id}" }).mkString(", ")
          val placeholder = columnValueOptSeq.map(_ => "?").mkString(", ")

          val insert = s"INSERT INTO user_table_lang_$tableId (id, langtag, $columns) VALUES (?, ?, $placeholder)"
          val binds = List(rowId, langtag) ::: columnValueOptSeq.map({ case (_, valueOpt) => valueOpt.orNull }).toList

          t.query(insert, Json.arr(binds: _*))
      }
    } else {
      // No values put into multilanguage columns, should be okay
      Future.successful(())
    }
  }

  private def createAttachments(
      tableId: TableId,
      rowId: RowId,
      values: Seq[(AttachmentColumn, Seq[(UUID, Option[Ordering])])]
  ): Future[_] = {
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

  import ModelHelper._

  def retrieveAnnotations(
      tableId: TableId,
      rowId: RowId,
      columns: Seq[ColumnType[_]]
  ): Future[(RowLevelAnnotations, CellLevelAnnotations)] = {
    for {
      result <- connection.query(
        s"SELECT ut.id, ${generateFlagsProjection(tableId)} FROM user_table_$tableId ut WHERE ut.id = ?",
        Json.arr(rowId))
    } yield {
      val rawRow = mapRowToRawRow(columns)(jsonArrayToSeq(selectNotNull(result).head))
      (rawRow.rowLevelAnnotations, rawRow.cellLevelAnnotations)
    }
  }

  def retrieve(tableId: TableId, rowId: RowId, columns: Seq[ColumnType[_]]): Future[RawRow] = {
    val projection = generateProjection(tableId, columns)
    val fromClause = generateFromClause(tableId)

    for {
      result <- connection
        .query(s"SELECT $projection FROM $fromClause WHERE ut.id = ? GROUP BY ut.id", Json.arr(rowId))
    } yield {
      mapRowToRawRow(columns)(jsonArrayToSeq(selectNotNull(result).head))
    }
  }

  def retrieveForeign(
      linkColumn: LinkColumn,
      rowId: RowId,
      foreignTableId: TableId,
      foreignColumns: Seq[ColumnType[_]],
      pagination: Pagination
  ): Future[Seq[RawRow]] = {
    val projection = generateProjection(foreignTableId, foreignColumns)
    val fromClause = generateFromClause(foreignTableId)

    val linkId = linkColumn.linkId
    val linkTable = s"link_table_$linkId"
    val linkDirection = linkColumn.linkDirection

    // linkColumn is from origin tables point of view
    // ... so we need to swap toSql and fromSql
    val cardinalityFilter =
      s"""
         |(SELECT COUNT(*) = 0 FROM $linkTable WHERE ${linkDirection.toSql} = ut.id AND ${linkDirection.fromSql} = ?) AND
         |(SELECT COUNT(*) FROM $linkTable WHERE ${linkDirection.toSql} = ut.id) < (SELECT ${linkDirection.fromCardinality} FROM system_link_table WHERE link_id = ?) AND
         |(SELECT COUNT(*) FROM $linkTable WHERE ${linkDirection.fromSql} = ?) < (SELECT ${linkDirection.toCardinality} FROM system_link_table WHERE link_id = ?)
       """.stripMargin

    for {
      result <- connection.query(
        s"SELECT $projection FROM $fromClause WHERE $cardinalityFilter GROUP BY ut.id ORDER BY ut.id $pagination",
        Json.arr(rowId, linkId, rowId, linkId)
      )
    } yield {
      resultObjectToJsonArray(result).map(jsonArrayToSeq).map(mapRowToRawRow(foreignColumns))
    }
  }

  def retrieveAll(tableId: TableId, columns: Seq[ColumnType[_]], pagination: Pagination): Future[Seq[RawRow]] = {
    val projection = generateProjection(tableId, columns)
    val fromClause = generateFromClause(tableId)

    for {
      result <- connection.query(s"SELECT $projection FROM $fromClause GROUP BY ut.id ORDER BY ut.id $pagination")
    } yield {
      resultObjectToJsonArray(result).map(jsonArrayToSeq).map(mapRowToRawRow(columns))
    }
  }

  private def mapRowToRawRow(columns: Seq[ColumnType[_]])(row: Seq[Any]): RawRow = {

    // Row should have at least = row_id, final_flag, cell_annotations
    assert(row.size >= 3)
    val liftedRow = row.lift

    (row.headOption, liftedRow(1), liftedRow(2)) match {
      case (Some(rowId: RowId), Some(finalFlag: Boolean), Some(cellAnnotationsStr)) =>
        val cellAnnotations =
          Option(cellAnnotationsStr).map(_.asInstanceOf[String]).map(Json.fromArrayString).getOrElse(Json.emptyArr())
        val rawValues = row.drop(3)

        RawRow(rowId,
               RowLevelAnnotations(finalFlag),
               CellLevelAnnotations(columns, cellAnnotations),
               mapResultRow(columns, rawValues))
      case _ =>
        // shouldn't happen b/c of assert
        throw UnknownServerException(s"Please check generateProjection!")
    }
  }

  def size(tableId: TableId): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM user_table_$tableId"

    connection.selectSingleValue(select)
  }

  private def mapValueByColumnType(column: ColumnType[_], value: Any): Any = {
    (column, Option(value)) match {
      case (MultiLanguageColumn(_: NumberColumn | _: CurrencyColumn), Some(obj)) =>
        val castedMap = Json
          .fromObjectString(obj.toString)
          .asMap
          .mapValues(Option(_))
          .mapValues({
            case None =>
              None.orNull
            case Some(v: String) =>
              Try(v.toInt)
                .orElse(Try(v.toDouble))
                .getOrElse(throw UnknownServerException(
                  s"invalid value in database, should be numeric string (column: $column)"))
            case Some(v) =>
              v
          })

        Json.obj(castedMap.toSeq: _*)

      case (MultiLanguageColumn(_), option) =>
        option
          .map(_.toString)
          .map(Json.fromObjectString)
          .getOrElse(Json.emptyObj())

      case (_: LinkColumn, option) =>
        option
          .map(_.toString)
          .map(Json.fromArrayString)
          .getOrElse(Json.emptyArr())

      case (_: NumberColumn | _: CurrencyColumn, Some(v: String)) =>
        Try(v.toInt)
          .orElse(Try(v.toDouble))
          .getOrElse(
            throw UnknownServerException(s"invalid value in database, should be a numeric string (column: $column)"))

      case _ =>
        value
    }
  }

  private def mapResultRow(columns: Seq[ColumnType[_]], rawValues: Seq[Any]): Seq[Any] = {
    val (concatColumnOpt, columnsAndRawValues) = columns.headOption match {
      case Some(c: ConcatColumn) =>
        (Some(c), (columns.drop(1), rawValues.drop(1)).zipped)
      case _ =>
        (None, (columns, rawValues).zipped)
    }

    concatColumnOpt match {
      case None =>
        // No concat column
        columnsAndRawValues.map(mapValueByColumnType)
      case Some(concatColumn) =>
        // Aggregate values for concat column
        val identifierColumns = columnsAndRawValues
          .filter({ (column: ColumnType[_], _) =>
            {
              concatColumn.columns.exists(_.id == column.id)
            }
          })
          .zipped

        import scala.collection.JavaConverters._

        val concatRawValues = identifierColumns
          .foldLeft(List.empty[Any])({
            case (joinedValues, (column, rawValue)) =>
              joinedValues.:+(mapValueByColumnType(column, rawValue))
          })
          .asJava

        (columns.drop(1), rawValues.drop(1)).zipped.map(mapValueByColumnType).+:(concatRawValues)
    }
  }

  private def generateFromClause(tableId: TableId): String = {
    s"user_table_$tableId ut LEFT JOIN user_table_lang_$tableId utl ON (ut.id = utl.id)"
  }

  private def generateProjection(tableId: TableId, columns: Seq[ColumnType[_]]): String = {
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
        generateLinkProjection(tableId, c)

      case _ =>
        "NULL"
    }

    Seq(Seq("ut.id"), Seq(generateFlagsProjection(tableId)), projection).flatten
      .mkString(",")
  }

  private def generateLinkProjection(tableId: TableId, c: LinkColumn): String = {
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

        // No distinct needed, just one rawValues
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
       |(SELECT
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
       |GROUP BY sub.${direction.fromSql}) AS column_${c.id}""".stripMargin
  }

  private def generateFlagsProjection(tableId: TableId): String = {
    s"""|ut.final AS final_flag,
        |(SELECT json_agg(
        |  json_build_object(
        |    'column_id', column_id,
        |    'uuid', uuid,
        |    'langtags', langtags::text[],
        |    'type', type,
        |    'value', value,
        |    'createdAt', ${parseDateTimeSql("created_at")}
        | )
        |) FROM (SELECT * FROM user_table_annotations_$tableId WHERE row_id = ut.id ORDER BY created_at) sub) AS cell_annotations""".stripMargin
  }
}
