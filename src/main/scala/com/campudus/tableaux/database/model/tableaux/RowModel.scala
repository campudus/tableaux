package com.campudus.tableaux.database.model.tableaux

import com.campudus.tableaux.{RowNotFoundException, UnknownServerException, UnprocessableEntityException}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{MultiLanguageColumn, _}
import com.campudus.tableaux.database.domain.DisplayInfos.Langtag
import com.campudus.tableaux.database.model.{Attachment, AttachmentFile, AttachmentModel}
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import org.vertx.scala.core.json.{Json, _}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging
import java.util.UUID
import org.joda.time.DateTime

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

sealed trait UpdateCreateRowModelHelper extends LazyLogging {
  protected[this] val connection: DatabaseConnection

  def rowExists(t: DbTransaction, tableId: TableId, rowId: RowId)(
      implicit ec: ExecutionContext
  ): Future[DbTransaction] = {
    t.selectSingleValue[Boolean](s"SELECT COUNT(*) = 1 FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
      .flatMap({
        case (t, true) => Future.successful(t)
        case (_, false) => Future.failed(RowNotFoundException(tableId, rowId))
      })
  }

  private def generateUnionSelectAndBinds(rowId: RowId, column: LinkColumn, toIds: Seq[RowId]): (String, Seq[Long]) = {
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

    (union, binds)
  }

  def getReplacedIds(
      tableId: TableId,
      rowId: RowId,
      maybeTransaction: Option[DbTransaction]
  )(implicit ec: ExecutionContext): Future[JsonArray] = {
    val query = s"SELECT COALESCE(replaced_ids, '[]') FROM user_table_${tableId} WHERE id = ?"
    val binds = Json.arr(rowId)

    for {
      res <- maybeTransaction match {
        case Some(transaction) => transaction.query(query, binds).map({ case (_, obj) => obj })
        case None => connection.transactional(t => t.query(query, binds))
      }

      replacedIds = Try(res.getJsonArray("results").getJsonArray(0).getString(0)) match {
        case Success(value) => Json.fromArrayString(value)
        case Failure(s) => Json.arr()
      }

    } yield (replacedIds)
  }

  def updateReplacedIds(
      tableId: TableId,
      replacedIds: JsonArray,
      oldRowId: RowId,
      newRowId: Int,
      maybeTransaction: Option[DbTransaction]
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val updateQuery = s"""
                         |UPDATE user_table_${tableId}
                         |SET replaced_ids = COALESCE(replaced_ids, '[]'::jsonb) || ?::jsonb
                         |WHERE id = ?
                         |""".stripMargin
    val binds = Json.arr(replacedIds.add(oldRowId).encode(), newRowId)

    for {
      _ <- maybeTransaction match {
        case Some(transaction) => transaction.query(updateQuery, binds)
        case None => connection.transactional(t => t.query(updateQuery, binds))
      }
    } yield ()
  }

  def updateLinks(
      table: Table,
      rowId: RowId,
      values: Seq[(LinkColumn, Seq[RowId])],
      maybeTransaction: Option[DbTransaction] = None
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val func = (value: (LinkColumn, Seq[RowId])) => {
      val (column, toIds) = value

      val linkId = column.linkId
      val direction = column.linkDirection

      val (union, binds) = generateUnionSelectAndBinds(rowId, column, toIds)

      val fnc = (t: DbTransaction) => {
        for {
          // check if row (where we want to add the links) really exists
          t <- rowExists(t, column.table.id, rowId)

          // check if "to-be-linked" rows really exist
          t <- toIds
            .foldLeft(Future(t))((futureT, toId) => {
              futureT.flatMap(t => rowExists(t, column.to.table.id, toId))
            })
            .recoverWith({ case ex: Throwable =>
              Future.failed(UnprocessableEntityException(ex.getMessage))
            })

          (t, _) <-
            if (toIds.nonEmpty) {
              t.query(
                s"INSERT INTO link_table_$linkId(${direction.fromSql}, ${direction.toSql}, ${direction.orderingSql}) $union RETURNING *",
                Json.arr(binds: _*)
              ).map({
                // if size doesn't match we hit the cardinality limit
                case (t, result) => {
                  (t, insertCheckSize(result, toIds.size))
                }
              })
            } else {
              Future.successful((t, Unit))
            }
        } yield (t, ())
      }
      maybeTransaction match {
        case None => connection.transactional(fnc)
        case Some(transaction) => fnc(transaction)
      }
    }

    values.foldLeft(Future(())) {
      (future, value) => future.flatMap(_ => func(value).map(_ => ()))
    }
  }

  def updateRowPermissions(tableId: TableId, rowId: RowId, rowPermissionsOpt: Option[RowPermissionSeq])(
      implicit ec: ExecutionContext
  ): Future[Option[RowPermissionSeq]] = {
    val updateQuery = s"""
                         |UPDATE user_table_${tableId}
                         |SET row_permissions = ?::jsonb
                         |WHERE id = ?
                         |""".stripMargin

    val value = rowPermissionsOpt match {
      case Some(rowPermissions) => Json.arr(rowPermissions: _*).encode()
      case None => null
    }

    for {
      _ <- connection.query(updateQuery, Json.arr(value, rowId))
    } yield rowPermissionsOpt
  }
}

class UpdateRowModel(val connection: DatabaseConnection) extends DatabaseQuery with UpdateCreateRowModelHelper {

  val attachmentModel = AttachmentModel(connection)

  def doQuery[A](
      transaction: Option[DbTransaction],
      queryString: String,
      args: JsonArray
  ): Future[JsonObject] = {
    transaction match {
      case Some(transaction) => transaction.query(queryString, args).map(res => res._2)
      case None => connection.query(queryString, args)
    }
  }

  def deleteRow(
      tableId: TableId,
      rowId: RowId,
      transaction: Option[DbTransaction] = None
  ): Future[Unit] = {
    for {
      result <- doQuery(
        transaction,
        s"DELETE FROM user_table_$tableId WHERE id = ?",
        Json.arr(rowId)
      )
    } yield {
      deleteNotNull(result)
    }
  }

  def clearRow(
      table: Table,
      rowId: RowId,
      columns: Seq[ColumnType[_]],
      deleteRowFn: (Table, RowId) => Future[EmptyObject],
      transaction: Option[DbTransaction] = None
  ): Future[Unit] = {

    val (simple, multis, links, attachments) =
      ColumnType.splitIntoTypes(columns)

    for {
      _ <- if (simple.isEmpty) Future.successful(()) else updateSimple(table, rowId, simple.map((_, None)), transaction)
      _ <- if (multis.isEmpty) Future.successful(()) else clearTranslation(table, rowId, multis, transaction)
      _ <- if (links.isEmpty) Future.successful(()) else clearLinks(table, rowId, links, deleteRowFn, transaction)
      _ <- if (attachments.isEmpty) Future.successful(()) else clearAttachments(table, rowId, attachments)
    } yield ()
  }

  def clearRowWithValues(
      table: Table,
      rowId: RowId,
      values: Seq[(ColumnType[_], _)],
      deleteRowFn: (Table, RowId) => Future[EmptyObject]
  ): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simple, multis, links, attachments)) =>
        // we only care about links because of delete cascade handling
        // for simple, multis, and attachments do same as in clearRow()
        for {
          _ <-
            if (simple.isEmpty) Future.successful(())
            else updateSimple(table, rowId, simple.map({ case (c, _) => (c, None) }))
          _ <- if (multis.isEmpty) Future.successful(()) else clearTranslation(table, rowId, multis.map(_._1))
          _ <- if (links.isEmpty) Future.successful(()) else clearLinksWithValues(table, rowId, links, deleteRowFn)
          _ <- if (attachments.isEmpty) Future.successful(()) else clearAttachments(table, rowId, attachments.map(_._1))
        } yield ()
    }
  }

  private def clearTranslation(
      table: Table,
      rowId: RowId,
      columns: Seq[SimpleValueColumn[_]],
      transaction: Option[DbTransaction] = None
  ): Future[_] = {
    val setExpression = columns
      .map({
        case MultiLanguageColumn(column) => s"column_${column.id} = NULL"
      })
      .mkString(", ")

    for {
      _ <- doQuery(
        transaction,
        s"UPDATE user_table_lang_${table.id} SET $setExpression WHERE id = ?",
        Json.arr(rowId)
      )
    } yield ()
  }

  private def clearLinksWithValues(
      table: Table,
      rowId: RowId,
      columnsWithValues: Seq[(LinkColumn, Seq[RowId])],
      deleteRowFn: (Table, RowId) => Future[EmptyObject],
      transaction: Option[DbTransaction] = None
  ): Future[_] = {
    val futureSequence = columnsWithValues.map({
      case (column, values) => {
        val fromIdColumn = column.linkDirection.fromSql

        val linkTable = s"link_table_${column.linkId}"

        for {
          _ <-
            if (column.linkDirection.constraint.deleteCascade) {
              deleteLinkedRows(table, rowId, column, deleteRowFn, values, transaction)
            } else {
              doQuery(
                transaction,
                s"DELETE FROM $linkTable WHERE $fromIdColumn = ?",
                Json.arr(rowId)
              )
            }
        } yield ()
      }
    })

    Future.sequence(futureSequence)
  }

  private def clearLinks(
      table: Table,
      rowId: RowId,
      columns: Seq[LinkColumn],
      deleteRowFn: (Table, RowId) => Future[EmptyObject],
      transaction: Option[DbTransaction] = None
  ): Future[_] = clearLinksWithValues(table, rowId, columns.map((_, Seq.empty)), deleteRowFn, transaction)

  private def clearAttachments(table: Table, rowId: RowId, columns: Seq[AttachmentColumn]): Future[_] = {
    val cleared = columns.map((c: AttachmentColumn) => attachmentModel.deleteAll(table.id, c.id, rowId))
    Future.sequence(cleared)
  }

  def updateArchiveLinkedRows(
      table: Table,
      rowId: RowId,
      column: LinkColumn,
      columnName: String,
      archivedFlagOpt: Boolean,
      archiveRowFn: (Table, RowId, String, Boolean) => Future[_]
  ) = {
    val linkTable = s"link_table_${column.linkId}"
    val fromIdColumn = column.linkDirection.fromSql
    val toIdColumn = column.linkDirection.toSql

    // TODO here we could end up in a endless loop,
    // TODO but we could argue that a schema like this doesn't make sense
    retrieveLinkedRows(table, rowId, column)
      .flatMap(foreignRowIds => {
        val futures = foreignRowIds
          .map(foreignRowId => {
            // only delete foreign row if it's not part of new values...
            // ... otherwise delete link
            // if (newForeignRowIds.contains(foreignRowId)) {
            //   doQuery(
            //     transaction,
            //     s"DELETE FROM $linkTable WHERE $fromIdColumn = ? AND $toIdColumn = ?",
            //     Json.arr(rowId, foreignRowId)
            //   )
            // } else {
            archiveRowFn(column.to.table, foreignRowId, columnName, archivedFlagOpt)
            // }
          })

        Future.sequence(futures)
      })
  }

  private def deleteLinkedRows(
      table: Table,
      rowId: RowId,
      column: LinkColumn,
      deleteRowFn: (Table, RowId) => Future[EmptyObject],
      newForeignRowIds: Seq[RowId] = Seq.empty,
      transaction: Option[DbTransaction] = None
  ): Future[_] = {
    val linkTable = s"link_table_${column.linkId}"
    val fromIdColumn = column.linkDirection.fromSql
    val toIdColumn = column.linkDirection.toSql

    // TODO here we could end up in a endless loop,
    // TODO but we could argue that a schema like this doesn't make sense
    retrieveLinkedRows(table, rowId, column)
      .flatMap(foreignRowIds => {
        val futures = foreignRowIds
          .map(foreignRowId => {
            // only delete foreign row if it's not part of new values...
            // ... otherwise delete link
            if (newForeignRowIds.contains(foreignRowId)) {
              doQuery(
                transaction,
                s"DELETE FROM $linkTable WHERE $fromIdColumn = ? AND $toIdColumn = ?",
                Json.arr(rowId, foreignRowId)
              )
            } else {
              deleteRowFn(column.to.table, foreignRowId)
            }
          })

        Future.sequence(futures)
      })
  }

  def retrieveLinkedRows(
      table: Table,
      rowId: RowId,
      column: LinkColumn
  ): Future[Seq[Long]] = {
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
    // these should be deleted then
    // do this in a recursive manner
    connection
      .query(selectForeignRows, Json.arr(rowId))
      .map(resultObjectToJsonArray)
      .map(_.map(_.getLong(0).asInstanceOf[Long]))
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
      _ <-
        if (column.linkDirection.constraint.deleteCascade) {
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
          (
            s"UPDATE $linkTable SET $orderColumn = (SELECT MIN($orderColumn) - 1 FROM $linkTable WHERE $rowIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ? AND $orderColumn > (SELECT MIN($orderColumn) FROM $linkTable WHERE $rowIdColumn = ?)",
            Json.arr(rowId, rowId, toId, rowId)
          )
        )
      case LocationEnd =>
        List(
          (
            s"UPDATE $linkTable SET $orderColumn = (SELECT MAX($orderColumn) + 1 FROM $linkTable WHERE $rowIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ? AND $orderColumn < (SELECT MAX($orderColumn) FROM $linkTable WHERE $rowIdColumn = ?)",
            Json.arr(rowId, rowId, toId, rowId)
          )
        )
      case LocationBefore(relativeTo) =>
        List(
          (
            s"UPDATE $linkTable SET $orderColumn = (SELECT $orderColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?) WHERE $rowIdColumn = ? AND $toIdColumn = ?",
            Json.arr(rowId, relativeTo, rowId, toId)
          ),
          (
            s"UPDATE $linkTable SET $orderColumn = $orderColumn + 1 WHERE ($orderColumn >= (SELECT $orderColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?) AND ($rowIdColumn = ? AND $toIdColumn != ?))",
            Json.arr(rowId, relativeTo, rowId, toId)
          )
        )
    }

    val normalize = (
      s"""
         |UPDATE $linkTable
         |SET $orderColumn = r.ordering
         |FROM (SELECT $rowIdColumn, $toIdColumn, row_number() OVER (ORDER BY $rowIdColumn, $orderColumn) AS ordering FROM $linkTable WHERE $rowIdColumn = ?) r
         |WHERE $linkTable.$rowIdColumn = r.$rowIdColumn AND $linkTable.$toIdColumn = r.$toIdColumn
       """.stripMargin,
      Json.arr(rowId)
    )

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
          t.query(
            s"SELECT $toIdColumn FROM $linkTable WHERE $rowIdColumn = ? AND $toIdColumn = ?",
            Json.arr(rowId, relativeTo)
          )
            .map({
              case (t, result) =>
                selectNotNull(result)
                t
            })

        case _ =>
          Future.successful(t)
      }

      (t, _) <- (listOfStatements :+ normalize).foldLeft(Future.successful((t, Vector[JsonObject]()))) {
        case (fTuple, (query, bindParams)) =>
          for {
            (latestTransaction, results) <- fTuple
            (lastT, result) <- latestTransaction.query(query, bindParams)
          } yield (lastT, results :+ result)
      }

      _ <- t.commit()
    } yield ()
  }

  def updateRow(
      table: Table,
      rowId: RowId,
      values: Seq[(ColumnType[_], _)],
      maybeTransaction: Option[DbTransaction] = None
  ): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simple, multis, links, attachments)) =>
        for {
          _ <- if (simple.isEmpty) Future.successful(()) else updateSimple(table, rowId, simple)
          _ <- if (multis.isEmpty) Future.successful(()) else updateTranslations(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else updateLinks(table, rowId, links, maybeTransaction)
          _ <- if (attachments.isEmpty) Future.successful(()) else updateAttachments(table, rowId, attachments)
        } yield ()
    }
  }

  private def updateSimple(
      table: Table,
      rowId: RowId,
      simple: List[(SimpleValueColumn[_], Option[Any])],
      transaction: Option[DbTransaction] = None
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
      update <- doQuery(
        transaction,
        s"UPDATE user_table_${table.id} SET $setExpression WHERE id = ?",
        Json.arr(binds: _*)
      )
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
              Json.arr(binds: _*)
            )
          } yield (t, result)
      }
    } else {
      // No values put into multilanguage columns, should be okay
      Future.successful(())
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
                addedOrUpdatedAttachment <-
                  if (currentAttachments.exists(_.file.file.uuid.equals(uuid))) {
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

  def updateRowsAnnotation(
      tableId: TableId,
      columnName: String,
      flag: Boolean
  ): Future[Unit] = {
    for {
      _ <- connection.query(s"UPDATE user_table_$tableId SET $columnName = ?", Json.arr(flag))
    } yield ()
  }

  def updateRowAnnotation(
      tableId: TableId,
      rowId: RowId,
      columnName: String,
      flag: Boolean
  ): Future[Unit] = {
    for {
      _ <- connection.query(s"UPDATE user_table_$tableId SET $columnName = ? WHERE id = ?", Json.arr(flag, rowId))
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
         |  user_table_annotations_$tableId.value = subquery.value AND
         |  user_table_annotations_$tableId.type = 'flag'
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

  def addRowPermissions(table: Table, row: Row, rowPermissions: RowPermissionSeq): Future[Option[RowPermissionSeq]] = {
    val mergedSeq = (row.rowPermissions.value ++ rowPermissions).distinct
    updateRowPermissions(table.id, row.id, Some(mergedSeq))
  }

  def deleteRowPermissions(table: Table, row: Row): Future[Option[RowPermissionSeq]] = {
    updateRowPermissions(table.id, row.id, None)
  }

  def replaceRowPermissions(
      table: Table,
      row: Row,
      rowPermissions: RowPermissionSeq
  ): Future[Option[RowPermissionSeq]] =
    updateRowPermissions(table.id, row.id, Some(rowPermissions))

}

class CreateRowModel(val connection: DatabaseConnection) extends DatabaseQuery with UpdateCreateRowModelHelper {
  val attachmentModel = AttachmentModel(connection)

  def createRow(
      table: Table,
      values: Seq[(ColumnType[_], _)],
      rowPermissionsOpt: Option[RowPermissionSeq]
  ): Future[RowId] = {
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
          _ <- rowPermissionsOpt match {
            case None => Future.successful(())
            case Some(rowPermissions) => updateRowPermissions(tableId, rowId, rowPermissionsOpt)
          }
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

class RetrieveRowModel(val connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {

  import ModelHelper._

  def rowExists(tableId: TableId, rowId: RowId)(implicit ec: ExecutionContext): Future[Boolean] = {
    connection.selectSingleValue[Boolean](s"SELECT COUNT(*) = 1 FROM user_table_$tableId WHERE id = ?", Json.arr(rowId))
  }

  def retrieveColumnValues(column: ShortTextColumn, langtagOpt: Option[Langtag]): Future[Seq[String]] = {
    val tableId = column.table.id

    val projection = column.languageType match {
      case LanguageNeutral => s"ut.column_${column.id}"
      case MultiLanguage | MultiCountry(_) => s"utl.column_${column.id}"
    }

    val (whereClause, bind) = (column.languageType, langtagOpt) match {
      case (LanguageNeutral, None) =>
        (s"(COALESCE(TRIM($projection), '') = '') IS FALSE", Json.arr())
      case (MultiLanguage, Some(langtag)) =>
        (s"langtag = ? AND (COALESCE(TRIM($projection), '') = '') IS FALSE", Json.arr(langtag))
      case (MultiLanguage, None) =>
        throw new IllegalArgumentException(
          "Column values from multi-language column can only be retrieved with a langtag"
        )
      case _ =>
        throw new IllegalArgumentException(
          "Column values can only be retrieved from shorttext language-neutral or multi-language columns"
        )
    }

    val query =
      s"SELECT DISTINCT $projection FROM ${generateFromClause(tableId)} WHERE $whereClause ORDER BY $projection"

    for {
      result <- connection.query(query, bind)
    } yield {
      resultObjectToJsonArray(result)
        .map(jsonArrayToSeq)
        .flatMap(_.headOption.map(_.toString))
    }
  }

  def retrieveTablesWithCellAnnotations(tables: Seq[Table])(
      implicit user: TableauxUser
  ): Future[Seq[TableWithCellAnnotations]] = {
    val query = tables
      .map({
        case Table(id, _, _, _, _, _, _, _) =>
          s"""SELECT
             |$id::bigint as table_id,
             |ua.row_id,
             |ua.column_id,
             |ua.uuid,
             |array_to_json(ua.langtags::text[]) AS langtags,
             |ua.type,
             |ua.value,
             |${parseDateTimeSql("ua.created_at")} AS created_at
             |FROM user_table_$id ut JOIN user_table_annotations_$id ua ON (ut.id = ua.row_id)""".stripMargin
      })
      .mkString("", " UNION ALL ", " ORDER BY table_id, row_id, column_id, created_at")

    for {
      result <- connection.query(query)
    } yield {
      resultObjectToJsonArray(result)
        .map(jsonArrayToSeq)
        .map({
          case Seq(
                tableId: TableId,
                rowId: RowId,
                columnId: ColumnId,
                uuidStr: String,
                langtags,
                annotationType: String,
                value,
                createdAtStr: String
              ) =>
            import scala.collection.JavaConverters._

            (
              tables.find(table => table.id == tableId).getOrElse(Table(tableId)),
              rowId,
              columnId,
              CellLevelAnnotation(
                UUID.fromString(uuidStr),
                CellAnnotationType(annotationType),
                Option(langtags)
                  .map(_.asInstanceOf[String])
                  .map(Json.fromArrayString(_).asScala.map(_.toString).toList)
                  .getOrElse(Seq.empty),
                Option(value).map(_.asInstanceOf[String]).orNull,
                DateTime.parse(createdAtStr)
              )
            )

          case invalidAnnotation =>
            throw UnknownServerException(s"Invalid annotation ($invalidAnnotation) stored in database.")
        })
        .groupBy({
          case (table: Table, _, _, _) => table.id
        })
        .toSeq
        .map({
          case (tableId: TableId, annotationsByTable) =>
            // take Table instance from first annotation
            // ... all Table objects are same b/c we grouped by table
            val table = annotationsByTable.headOption
              .map({
                case (table: Table, _, _, _) => table
              })
              // better safe than sorry
              .getOrElse(Table(tableId))

            val groupedAnnotations = annotationsByTable
              .groupBy({
                case (_, rowId: RowId, _, _) => rowId
              })
              .map({
                case (rowId: RowId, annotationsByRow) =>
                  rowId ->
                    annotationsByRow
                      .groupBy({
                        case (_, _, columnId: ColumnId, _) => columnId
                      })
                      .mapValues(_.map({
                        case (_, _, _, annotation) => annotation
                      }))
              })

            TableWithCellAnnotations(table, groupedAnnotations)
        })
    }
  }

  def retrieveCellAnnotationCount(tables: Seq[TableId]): Future[Map[TableId, Seq[CellAnnotationCount]]] = {

    /**
      * If type is not flag we ignore the value. Other types (info, warning, error) are comment annotations. Their
      * values are always different. The value of flag annotations is important because it describes what sort of flag
      * annotation it is (needs_translation, important, check-me, later).
      */
    val query = tables
      .map({ tableId =>
        s"""|SELECT
            |$tableId::bigint as table_id,
            |ua.type,
            |CASE WHEN type = 'flag' THEN value END AS type_value,
            |sub.langtag AS langtag,
            |COUNT(*),
            |${parseDateTimeSql("MAX(ua.created_at)")} AS last_created_at
            |FROM user_table_annotations_$tableId ua
            |LEFT JOIN LATERAL UNNEST(ua.langtags) AS sub(langtag) ON ua.langtags <> '{}'
            |WHERE NOT (type = 'flag' AND value = 'needs_translation' AND (langtags = '{}' OR langtags IS NULL))
            |GROUP BY type, type_value, langtag""".stripMargin
      })
      .mkString("", " UNION ALL ", " ORDER BY table_id, type, type_value, last_created_at DESC")

    for {
      result <-
        if (tables.isEmpty) {
          Future.successful(Json.emptyObj())
        } else {
          connection.query(query)
        }
    } yield {
      resultObjectToJsonArray(result)
        .map(jsonArrayToSeq)
        .map({
          case Seq(tableId: TableId, annotationType: String, value, langtag, count: Long, lastCreatedAtStr: String) =>
            (
              tableId,
              CellAnnotationCount(
                CellAnnotationType(annotationType),
                Option(value).map(_.asInstanceOf[String]),
                Option(langtag).map(_.asInstanceOf[String]),
                count,
                DateTime.parse(lastCreatedAtStr)
              )
            )

          case invalidRow =>
            throw UnknownServerException(s"Invalid aggregated annotation count ($invalidRow) stored in database.")
        })
        .groupBy({
          case (tableId: TableId, _) => tableId
        })
        .mapValues(_.map({
          case (_, annotationTypeCount: CellAnnotationCount) => annotationTypeCount
        }))
    }
  }

  def retrieveAnnotations(
      tableId: TableId,
      rowId: RowId,
      columns: Seq[ColumnType[_]]
  ): Future[(RowLevelAnnotations, RowPermissions, CellLevelAnnotations)] = {
    for {
      result <- connection.query(
        s"SELECT ut.id, ${generateFlagsAndAnnotationsProjection(tableId)} FROM user_table_$tableId ut WHERE ut.id = ?",
        Json.arr(rowId)
      )
    } yield {
      val rawRow = mapRowToRawRow(columns)(jsonArrayToSeq(selectNotNull(result).head))
      (rawRow.rowLevelAnnotations, rawRow.rowPermissions, rawRow.cellLevelAnnotations)
    }
  }

  def retrieveAnnotation(
      tableId: TableId,
      rowId: RowId,
      column: ColumnType[_],
      uuid: UUID
  ): Future[Option[CellLevelAnnotation]] = {
    for {
      (_, _, cellLevelAnnotations) <- retrieveAnnotations(tableId, rowId, Seq(column))
      annotations = cellLevelAnnotations.annotations.get(column.id).getOrElse(Seq.empty[CellLevelAnnotation])
    } yield annotations.find(_.uuid == uuid)

  }

  def retrieveRowPermissions(tableId: TableId, rowId: RowId): Future[RowPermissions] = {
    for {
      (_, rowPermissions, _) <- retrieveAnnotations(tableId, rowId, Seq()).recover({
        case _ =>
          (RowLevelAnnotations(false, false), RowPermissions(Json.arr()), CellLevelAnnotations(Seq(), Json.arr()))
      })
    } yield {
      rowPermissions
    }
  }

  def retrieveRowsPermissions(tableId: TableId, rowIds: Seq[RowId]): Future[Seq[RowPermissions]] = {
    Future.sequence(rowIds.map(retrieveRowPermissions(tableId, _)))
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
      foreignColumns: Seq[ColumnType[_]],
      pagination: Pagination,
      linkDirection: LinkDirection
  ): Future[Seq[RawRow]] = {
    val foreignTableId = linkColumn.to.table.id

    val projection = generateProjection(foreignTableId, foreignColumns)
    val fromClause = generateFromClause(foreignTableId)
    val cardinalityFilter = generateCardinalityFilter(linkColumn)
    val shouldNotCheckCardinality = linkDirection.isManyToMany

    for {
      maybeCardinalityFilter <-
        if (shouldNotCheckCardinality) {
          Future.successful("")
        } else {
          Future.successful(s"WHERE $cardinalityFilter")
        }
      result <- connection.query(
        s"SELECT $projection FROM $fromClause $maybeCardinalityFilter GROUP BY ut.id ORDER BY ut.id $pagination",
        if (shouldNotCheckCardinality) {
          Json.arr()
        } else {
          Json.arr(rowId, linkColumn.linkId, rowId, linkColumn.linkId)
        }
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

    // Row should have at least = row_id, final_flag, archived_flag, cell_annotations, row_permissions
    assert(row.size >= 5)
    val liftedRow = row.lift

    (liftedRow(0), liftedRow(1), liftedRow(2), liftedRow(3), liftedRow(4)) match {
      // values in case statement are nullable even if they are wrapped in Some, see lift function of Seq
      case (
            Some(rowId: RowId),
            Some(finalFlag: Boolean),
            Some(archivedFlag: Boolean),
            Some(cellAnnotationsStr),
            Some(permissionsStr)
          ) =>
        val cellAnnotations = Option(cellAnnotationsStr)
          .map(_.asInstanceOf[String])
          .map(Json.fromArrayString)
          .getOrElse(Json.emptyArr())
        val rawValues = row.drop(5)

        val rowPermissions = Option(permissionsStr) match {
          case Some(permissionsArrayString: String) => new JsonArray(permissionsArrayString)
          case _ => Json.arr()
        }

        RawRow(
          rowId,
          RowLevelAnnotations(finalFlag, archivedFlag),
          RowPermissions(rowPermissions),
          CellLevelAnnotations(columns, cellAnnotations),
          (columns, rawValues).zipped.map(mapValueByColumnType)
        )
      case _ =>
        throw UnknownServerException(s"Please check generateProjection!")
    }
  }

  def size(tableId: TableId): Future[Long] = {
    val select = s"SELECT COUNT(*) FROM user_table_$tableId"

    connection.selectSingleValue(select)
  }

  def sizeForeign(linkColumn: LinkColumn, rowId: RowId, linkDirection: LinkDirection): Future[Long] = {
    val foreignTableId = linkColumn.to.table.id
    val cardinalityFilter = generateCardinalityFilter(linkColumn)
    val shouldNotCheckCardinality = linkDirection.isManyToMany

    for {
      maybeCardinalityFilter <-
        if (shouldNotCheckCardinality) {
          Future.successful("")
        } else {
          Future.successful(s"WHERE $cardinalityFilter")
        }
      result <- connection.selectSingleValue[Long](
        s"SELECT COUNT(*) FROM user_table_$foreignTableId ut $maybeCardinalityFilter",
        if (shouldNotCheckCardinality) {
          Json.arr()
        } else {
          Json.arr(rowId, linkColumn.linkId, rowId, linkColumn.linkId)
        }
      )
    } yield { result }
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
                  s"invalid value in database, should be numeric string (column: $column)"
                ))
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
            throw UnknownServerException(s"invalid value in database, should be a numeric string (column: $column)")
          )

      case _ =>
        value
    }
  }

  private def generateFromClause(tableId: TableId): String = {
    s"user_table_$tableId ut LEFT JOIN user_table_lang_$tableId utl ON (ut.id = utl.id)"
  }

  private def generateProjection(tableId: TableId, columns: Seq[ColumnType[_]]): String = {
    val projection = columns map {
      case _: ConcatColumn | _: AttachmentColumn | _: GroupColumn =>
        // Values will be generated/fetched while post-processing raw rows
        // see TableauxModel.mapRawRows
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

    Seq(Seq("ut.id"), Seq(generateFlagsAndAnnotationsProjection(tableId)), projection).flatten
      .mkString(",")
  }

  private def generateLinkProjection(tableId: TableId, c: LinkColumn): String = {
    val linkId = c.linkId
    val direction = c.linkDirection
    val toTableId = c.to.table.id

    val (column, value) = c.to match {
      case _: ConcatenateColumn =>
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

      // no case needed for AttachmentColumn yet
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

  private def generateFlagsAndAnnotationsProjection(tableId: TableId): String = {
    s"""|ut.final AS final_flag,
        |ut.archived AS archived_flag,
        |(SELECT json_agg(
        |  json_build_object(
        |    'column_id', column_id,
        |    'uuid', uuid,
        |    'langtags', langtags::text[],
        |    'type', type,
        |    'value', value,
        |    'createdAt', ${parseDateTimeSql("created_at")}
        | )
        |) FROM (SELECT column_id, uuid, langtags, type, value, created_at FROM user_table_annotations_$tableId WHERE row_id = ut.id ORDER BY created_at) sub) AS cell_annotations,
        |ut.row_permissions AS row_permissions""".stripMargin
  }

  private def generateCardinalityFilter(linkColumn: LinkColumn): String = {
    val linkId = linkColumn.linkId
    val linkTable = s"link_table_$linkId"
    val linkDirection = linkColumn.linkDirection

    // linkColumn is from origin tables point of view
    // ... so we need to swap toSql and fromSql
    s"""
       |(SELECT COUNT(*) = 0 FROM $linkTable WHERE ${linkDirection.toSql} = ut.id AND ${linkDirection.fromSql} = ?) AND
       |(SELECT COUNT(*) FROM $linkTable WHERE ${linkDirection.toSql} = ut.id) < (SELECT ${linkDirection.fromCardinality} FROM system_link_table WHERE link_id = ?) AND
       |(SELECT COUNT(*) FROM $linkTable WHERE ${linkDirection.fromSql} = ?) < (SELECT ${linkDirection.toCardinality} FROM system_link_table WHERE link_id = ?)
       """.stripMargin
  }
}
