package com.campudus.tableaux.database.model.tableaux

import java.util.UUID

import com.campudus.tableaux.ArgumentChecker
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel._
import com.campudus.tableaux.database.model.{Attachment, AttachmentFile, AttachmentModel}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{Json, _}

import scala.concurrent.Future

class RowModel(val connection: DatabaseConnection) extends DatabaseQuery {
  val attachmentModel = AttachmentModel(connection)

  private def splitIntoDatabaseTypes(values: Seq[(ColumnType[_], _)]): (
    List[(SimpleValueColumn[_], _)],
      List[(MultiLanguageColumn[_], Map[String, _])],
      List[(LinkColumn[_], _)],
      List[(AttachmentColumn, _)]
    ) = {

    values.foldLeft((
      List[(SimpleValueColumn[_], _)](),
      List[(MultiLanguageColumn[_], Map[String, _])](),
      List[(LinkColumn[_], _)](),
      List[(AttachmentColumn, _)]())) {
      case ((s, m, l, a), (c: SimpleValueColumn[_], v)) => ((c, v) :: s, m, l, a)
      case ((s, m, l, a), (c: MultiLanguageColumn[_], v: JsonObject)) =>
        logger.info(s"got a multilanguage column $c -> $v")
        import scala.collection.JavaConverters._
        val valueAsMap = v.getMap.asScala.toMap
        (s, (c, valueAsMap) :: m, l, a)
      case ((s, m, l, a), (c: LinkColumn[_], v)) => (s, m, (c, v) :: l, a)
      case ((s, m, l, a), (c: AttachmentColumn, v)) => (s, m, l, (c, v) :: a)
      case (_, (c, v)) =>
        logger.info(s"Class cast exception: unknown column or value: $c -> $v")
        throw new ClassCastException(s"unknown column or value: $c -> $v")
    }
  }

  def createRow(tableId: TableId, values: Seq[(ColumnType[_], _)]): Future[RowId] = {
    val (simple, multis, links, attachments) = splitIntoDatabaseTypes(values)

    for {
      rowId <- if (simple.isEmpty) createEmpty(tableId) else createFull(tableId, simple)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslations(tableId, rowId, multis)
      _ <- if (links.isEmpty) Future.successful(()) else createLinks(tableId, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else createAttachments(tableId, rowId, attachments)
    } yield rowId
  }

  def createEmpty(tableId: TableId): Future[RowId] = {
    for {
      result <- connection.query(s"INSERT INTO user_table_$tableId DEFAULT VALUES RETURNING id")
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  private def createFull(tableId: TableId, values: Seq[(SimpleValueColumn[_], _)]): Future[RowId] = {
    val placeholder = values.map(_ => "?").mkString(", ")
    val columns = values.map { case (column: ColumnType[_], _) => s"column_${column.id}" }.mkString(", ")
    val binds = values.map { case (_, value) => value }

    for {
      result <- connection.query(s"INSERT INTO user_table_$tableId ($columns) VALUES ($placeholder) RETURNING id", Json.arr(binds: _*))
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  private def createLinks(tableId: TableId, rowId: RowId, values: Seq[(LinkColumn[_], _)]): Future[_] = {
    val futureSequence = for {
      (column: LinkColumn[_], idValues) <- values
      toIds = idValues match {
        case x: JsonObject => Seq(x.getLong("to").toLong)
        case x: JsonArray =>
          import scala.collection.JavaConverters._
          logger.info(s"hello x JsonArray: ${x.encode()}")
          x.asScala.map(_.asInstanceOf[JsonObject].getLong("id").toLong).toSeq
      }
    } yield {
      logger.info(s"link column = $column")
      logger.info(s"toIds = $toIds")
      val linkId = column.linkInformation._1
      val id1 = column.linkInformation._2
      val id2 = column.linkInformation._3

      val paramStr = toIds.map(_ => s"SELECT ?, ? WHERE NOT EXISTS (SELECT $id1, $id2 FROM link_table_$linkId WHERE $id1 = ? AND $id2 = ?)").mkString(" UNION ")
      val params = toIds.flatMap(to => List(rowId, to, rowId, to))

      for {
        t <- connection.begin()

        (t, _) <- t.query(s"DELETE FROM link_table_$linkId WHERE $id1 = ?", Json.arr(rowId))

        (t, _) <- {
          if (params.nonEmpty) {
            t.query(s"INSERT INTO link_table_$linkId($id1, $id2) $paramStr", Json.arr(params: _*))
          } else {
            Future((t, Json.emptyObj()))
          }
        }
        _ <- t.commit()
      } yield ()
    }

    Future.sequence(futureSequence)
  }

  private def createAttachments(tableId: TableId, rowId: RowId, values: Seq[(AttachmentColumn, _)]): Future[_] = {
    import ArgumentChecker._
    val futureSequence = for {
      (column: AttachmentColumn, attachmentValue) <- values
      attachments = attachmentValue match {
        case x: JsonObject =>
          logger.info(s"attachmentValue=${x.encode()}")
          notNull(x.getString("uuid"), "uuid")
          Seq(Attachment(
            tableId,
            column.id,
            rowId,
            UUID.fromString(x.getString("uuid")),
            Option(x.getLong("ordering")).map(_.toLong)))
        case x: JsonArray =>
          import scala.collection.JavaConverters._
          x.asScala.map {
            case file: JsonObject =>
              notNull(file.getString("uuid"), "uuid")
              Attachment(tableId, column.id, rowId, UUID.fromString(file.getString("uuid")), Option(file.getLong("ordering")))
          }.toSeq
        case x: Stream[_] =>
          x.map {
            case file: AttachmentFile =>
              Attachment(tableId, column.id, rowId, file.file.file.uuid.get, Some(file.ordering))
          }.toSeq
      }
    } yield {
      attachmentModel.replace(tableId, column.id, rowId, attachments)
    }

    Future.sequence(futureSequence)
  }

  private def createTranslations(tableId: TableId, rowId: RowId, values: Seq[(MultiLanguageColumn[_], Map[String, _])]): Future[_] = {
    val entries = for {
      (column, langs) <- values
      (langTag: String, value) <- langs
    } yield (langTag, (column, value))

    val columnsForLang = entries.groupBy(_._1).mapValues(_.map(_._2))

    connection.transactionalFoldLeft(columnsForLang.toSeq) { (t, _, langValues) =>
      val columns = langValues._2.map { case (col, _) => s"column_${col.id}" }.mkString(", ")
      val placeholder = langValues._2.map(_ => "?").mkString(", ")
      val insertStmt = s"INSERT INTO user_table_lang_$tableId (id, langtag, $columns) VALUES (?, ?, $placeholder)"
      val insertBinds = List(rowId, langValues._1) ::: langValues._2.map(_._2).toList

      for {
        (t, result) <- t.query(insertStmt, Json.arr(insertBinds: _*))
      } yield (t, result)
    }
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
      getSeqOfJsonArray(result).map(jsonArrayToSeq).map {
        row =>
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

  private def joinMultiLanguageJson(fields: Seq[String], joined: Map[String, List[AnyRef]], obj: AnyRef): Map[String, List[AnyRef]] = {
    import scala.collection.JavaConversions._

    fields.foldLeft(joined)({
      (result, field) =>
        val joinedValue = result.get(field)

        val value = obj match {
          case a: JsonArray => a.toList.map({
            case o: JsonObject =>
              o.getJsonObject("value").getValue(field)
          })
          case o: JsonObject => o.getValue(field)
          case _ => obj
        }

        val merged = (joinedValue, value) match {
          case (None, null) => List[AnyRef](null)
          case (None, _) => List[AnyRef](value)
          case (Some(j), null) => j.:+(null)
          case (Some(j), _) => j.:+(value)
        }

        result.+((field, merged))
    })
  }

  private def mapValueByColumnType(column: ColumnType[_], value: AnyRef): AnyRef = {
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

      case _ =>
        value
    }
  }

  private def mapResultRow(columns: Seq[ColumnType[_]], result: Seq[AnyRef]): Seq[AnyRef] = {
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

        val joinedValue = identifierColumns.foldLeft(List.empty[AnyRef])({
          (joined, columnValue) =>
            joined.:+(mapValueByColumnType(columnValue._1, columnValue._2))
        }).asJava

        (columns.drop(1), result.drop(1)).zipped.map(mapValueByColumnType).+:(joinedValue)
    }
  }

  // TODO should be possible to kick columns here
  private def generateFromClause(tableId: TableId, columns: Seq[ColumnType[_]]): String = {
    s"user_table_$tableId ut LEFT JOIN user_table_lang_$tableId utl ON (ut.id = utl.id)"
  }

  private val dateTimeFormat = "YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\""
  private val dateFormat = "YYYY-MM-DD"

  private def parseDateTimeSql(column: String): String = {
    s"""TO_CHAR($column AT TIME ZONE 'UTC', '$dateTimeFormat')"""
  }

  private def parseDateSql(column: String): String = {
    s"TO_CHAR($column, '$dateFormat')"
  }

  private def generateProjection(columns: Seq[ColumnType[_]]): String = {
    val projection = columns map {
      case _: ConcatColumn | _: AttachmentColumn =>
        // Values will be calculated or added after select
        "NULL"

      case c: MultiLanguageColumn[_] =>
        val column = c match {
          case _: MultiDateTimeColumn =>
            parseDateTimeSql(s"utl.column_${c.id}")
          case _: MultiDateColumn =>
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

      case c: LinkColumn[_] =>
        val (linkId, id1, id2) = c.linkInformation
        val toTableId = c.to.table.id

        val column = c.to match {
          case _: ConcatColumn =>
            (s"''", "NULL")

          case to: MultiLanguageColumn[_] =>
            val linkedColumn = to match {
              case _: MultiDateTimeColumn =>
                parseDateTimeSql(s"utl$toTableId.column_${to.id}")
              case _: MultiDateColumn =>
                parseDateSql(s"utl$toTableId.column_${to.id}")
              case _ =>
                s"utl$toTableId.column_${to.id}"
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
            | json_agg(sub.column_${c.to.id})
            |FROM
            |(
            | SELECT
            |   lt$linkId.$id1 AS $id1,
            |   json_build_object('id', ut$toTableId.id, 'value', ${column._2}) AS column_${c.to.id}
            | FROM
            |   link_table_$linkId lt$linkId JOIN
            |   user_table_$toTableId ut$toTableId ON (lt$linkId.$id2 = ut$toTableId.id)
            |   LEFT JOIN user_table_lang_$toTableId utl$toTableId ON (ut$toTableId.id = utl$toTableId.id)
            | WHERE ${column._1} IS NOT NULL
            | GROUP BY ut$toTableId.id, lt$linkId.$id1
            | ORDER BY ut$toTableId.id
            |) sub
            |WHERE sub.$id1 = ut.id
            |GROUP BY sub.$id1
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
