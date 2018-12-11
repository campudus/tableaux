package com.campudus.tableaux.database.model

import com.campudus.tableaux.{InvalidRequestException, RowNotFoundException}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.database.model.structure.TableModel
import com.campudus.tableaux.helper.IdentifierFlattener
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{JsonObject, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieve(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      langtagOpt: Option[String]
  ): Future[SeqCellHistory] = {

    val whereLanguage = (column.languageType, langtagOpt) match {
      case (MultiLanguage, Some(langtag)) => s" AND (value -> 'value' -> '$langtag')::json IS NOT NULL"
      case (_, None) => ""
      case (_, Some(_)) =>
        throw new InvalidRequestException(
          "History values filtered by langtags can only be retrieved from multi-language columns")
    }

    val select =
      s"""
         |  SELECT
         |    revision,
         |    event,
         |    column_type,
         |    multilanguage,
         |    author,
         |    timestamp,
         |    value
         |  FROM
         |    user_table_history_${table.id}
         |  WHERE
         |    row_id = ?
         |    AND column_id = ?
         |    AND event = 'cell_changed'
         |    $whereLanguage
         |  ORDER BY
         |    timestamp ASC
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(rowId, column.id))
    } yield {
      val cellHistory = resultObjectToJsonArray(result).map(mapToCellHistory)
      SeqCellHistory(cellHistory)
    }
  }

  private def mapToCellHistory(row: JsonArray): CellHistory = {

    def parseJson(jsonString: String): JsonObject = {
      jsonString match {
        case null => Json.emptyObj()

        case _ =>
          Try(Json.fromObjectString(jsonString)) match {
            case Success(json) => json
            case Failure(_) =>
              logger.error(s"Couldn't parse json. Excepted JSON but got: $jsonString")
              Json.emptyObj()
          }
      }
    }

    CellHistory(
      row.getLong(0),
      row.getString(1),
      TableauxDbType(row.getString(2)),
      LanguageType(Option(row.getString(3))),
      row.getString(4),
      convertStringToDateTime(row.getString(5)),
      parseJson(row.getString(6))
    )
  }
}

sealed trait CreateHistoryModelBase extends DatabaseQuery {
  protected[this] val tableauxModel: TableauxModel
  protected[this] val connection: DatabaseConnection

  val tableModel = new TableModel(connection)

  protected def retrieveCurrentLinkIds(
      table: Table,
      col: LinkColumn,
      rowId: RowId
  ): Future[Seq[RowId]] = {
    for {
      cell <- tableauxModel.retrieveCell(table, col.id, rowId)
    } yield {
      import scala.collection.JavaConverters._

      cell.getJson
        .getJsonArray("value")
        .asScala
        .map(_.asInstanceOf[JsonObject])
        .map(_.getLong("id").longValue())
        .toSeq
    }
  }

  protected def createLinks(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])],
      replace: Boolean
  ): Future[Seq[Any]] = {

    def wrapValue(valueSeq: Seq[(_, RowId, Object)]): JsonObject = {
      Json.obj(
        "value" ->
          valueSeq.map({
            case (_, rowId, value) => Json.obj("id" -> rowId, "value" -> value)
          })
      )
    }

    def retrieveLinkIdsToPut(
        col: LinkColumn,
        linkIdsToPutOrAdd: Seq[RowId]
    ): Future[Seq[RowId]] = {
      if (replace)
        Future.successful(linkIdsToPutOrAdd)
      else
        for {
          currentLinkIds <- retrieveCurrentLinkIds(table, col, rowId)
        } yield {
          // In some cases the new links are already inserted, in other cases not.
          // To be on the safe side, we only add them if they haven't been added yet.
          val diffSeq = linkIdsToPutOrAdd.diff(currentLinkIds)
          currentLinkIds.union(diffSeq)
        }
    }

    Future.sequence(
      links.map({
        case (col, linkIdsToPutOrAdd) => {
          if (linkIdsToPutOrAdd.isEmpty) {
            insertHistory(table, rowId, col.id, col.kind, col.languageType, wrapValue(Seq()))
          } else {
            for {
              linkIds <- retrieveLinkIdsToPut(col, linkIdsToPutOrAdd)
              cellSeq <- retrieveForeignIdentifierCells(col, linkIds)
              langTags <- tableModel.retrieveGlobalLangtags()
            } yield {
              val preparedData = cellSeq.map(cell => {
                IdentifierFlattener.compress(langTags, cell.value) match {
                  case Right(m) => (MultiLanguage, cell.rowId, m)
                  case Left(s) => (LanguageNeutral, cell.rowId, s)
                }
              })

              val languageType = preparedData.head._1
              insertHistory(table, rowId, col.id, col.kind, languageType, wrapValue(preparedData))
            }
          }
        }
      })
    )
  }

  private def retrieveForeignIdentifierCells(
      col: LinkColumn,
      linkIds: Seq[RowId]
  ): Future[Seq[Cell[Any]]] = {
    val linkedCellSeq = linkIds.map(
      linkId =>
        for {
          foreignIdentifier <- tableauxModel.retrieveCell(col.to.table, col.to.id, linkId)
        } yield foreignIdentifier
    )
    Future.sequence(linkedCellSeq)
  }

  protected def createTranslation(
      table: Table,
      rowId: RowId,
      values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]
  ): Future[Seq[RowId]] = {

    def wrapLanguageValue(langtag: String, value: Any): JsonObject = Json.obj("value" -> Json.obj(langtag -> value))

    val entries = for {
      (column, langtagValueOptMap) <- values
      (langtag: String, valueOpt) <- langtagValueOptMap
    } yield (langtag, (column, valueOpt))

    val columnsForLang = entries
      .groupBy({ case (langtag, _) => langtag })
      .mapValues(_.map({ case (_, columnValueOpt) => columnValueOpt }))

    val futureSequence: Seq[Future[RowId]] = columnsForLang.toSeq
      .flatMap({
        case (langtag, columnValueOptSeq) =>
          val languageCellEntries = columnValueOptSeq
            .map({
              case (column: SimpleValueColumn[_], valueOpt) =>
                (column.id, column.kind, column.languageType, wrapLanguageValue(langtag, valueOpt.orNull))
            })

          languageCellEntries.map({
            case (columnId, columnType, languageType, value) =>
              insertHistory(table, rowId, columnId, columnType, languageType, value)
          })
      })

    Future.sequence(futureSequence)
  }

  protected def createSimple(
      table: Table,
      rowId: RowId,
      simples: Seq[(SimpleValueColumn[_], Option[Any])]
  ): Future[Seq[RowId]] = {

    def wrapValue(value: Any): JsonObject = Json.obj("value" -> value)

    val cellEntries = simples
      .map({
        case (column: SimpleValueColumn[_], valueOpt) =>
          (column.id, column.kind, column.languageType, wrapValue(valueOpt.orNull))
      })

    val futureSequence: Seq[Future[RowId]] = cellEntries
      .map({
        case (columnId, columnType, languageType, value) =>
          insertHistory(table, rowId, columnId, columnType, languageType, value)
      })

    Future.sequence(futureSequence)
  }

  private def insertHistory(
      table: Table,
      rowId: RowId,
      columnId: ColumnId,
      columnType: TableauxDbType,
      languageType: LanguageType,
      json: JsonObject
  ): Future[RowId] = {
    logger.info(s"createHistory ${table.id} $columnId $rowId $json")
    for {
      result <- connection.query(
        s"""INSERT INTO
           |  user_table_history_${table.id}
           |    (row_id, column_id, column_type, multilanguage, value)
           |  VALUES
           |    (?, ?, ?, ?, ?)
           |  RETURNING revision""".stripMargin,
        Json.arr(rowId, columnId, columnType.toString, languageType.toString, json.toString)
      )
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

}

case class CreateInitialHistoryModel(
    protected[this] val tableauxModel: TableauxModel,
    protected[this] val connection: DatabaseConnection
) extends DatabaseQuery
    with CreateHistoryModelBase {

  def createHistoryIfNotExists(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, links)
          //          _ <- if (attachments.isEmpty) Future.successful(()) else createSimple(table, columnId, rowId, simple)
        } yield ()
    }
  }

  def createLinksInit(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])],
  ): Future[Seq[Unit]] = {

    Future.sequence(links.map({
      case (column, _) => {
        for {
          historyEntryExists <- historyExists(table.id, column.id, rowId)
          _ <- if (!historyEntryExists) {
            for {
              linkIds <- retrieveCurrentLinkIds(table, column, rowId)
              _ <- if (linkIds.nonEmpty)
                createLinks(table, rowId, Seq((column, linkIds)), true)
              else
                Future.successful(())
            } yield ()
          } else
            Future.successful(())
        } yield ()
      }
    }))
  }

  private def createSimpleInit(
      table: Table,
      rowId: RowId,
      simples: Seq[(SimpleValueColumn[_], Option[Any])]
  ): Future[Seq[Unit]] = {

    def createIfNotEmpty(column: SimpleValueColumn[_]) = {
      for {
        value <- retrieveCellValue(table, column, rowId)
      } yield {
        value match {
          case Some(v) => createSimple(table, rowId, Seq((column, Option(v))))
          case None => Future.successful(())
        }
      }
    }

    Future.sequence(simples.map({
      case (column: SimpleValueColumn[_], _) =>
        for {
          historyEntryExists <- historyExists(table.id, column.id, rowId)
          _ <- if (!historyEntryExists) createIfNotEmpty(column) else Future.successful(())
        } yield ()
    }))
  }

  private def createTranslationInit(
      table: Table,
      rowId: RowId,
      values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]
  ): Future[Seq[Unit]] = {

    val entries = for {
      (column, langtagValueOptMap) <- values
      (langtag: String, valueOpt) <- langtagValueOptMap
    } yield (langtag, (column, valueOpt))

    val columnsForLang = entries
      .groupBy({ case (langtag, _) => langtag })
      .mapValues(_.map({ case (_, columnValueOpt) => columnValueOpt }))

    def createIfNotEmpty(column: SimpleValueColumn[_], langtag: String) = {
      for {
        value <- retrieveCellValue(table, column, rowId, Option(langtag))
      } yield {
        value match {
          case Some(v) => createTranslation(table, rowId, Seq((column, Map(langtag -> Option(v)))))
          case None => Future.successful(())
        }
      }
    }

    Future.sequence(
      columnsForLang.toSeq
        .flatMap({
          case (langtag, columnValueOptSeq) => {
            columnValueOptSeq
              .map({
                case (column: SimpleValueColumn[_], _) =>
                  for {
                    historyEntryExists <- historyExists(table.id, column.id, rowId, Option(langtag))
                    _ <- if (!historyEntryExists) createIfNotEmpty(column, langtag) else Future.successful(())
                  } yield ()
              })
          }
        }))
  }

  private def historyExists(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      langtagOpt: Option[String] = None
  )(implicit ec: ExecutionContext): Future[Boolean] = {

    val whereLanguage = langtagOpt match {
      case Some(langtag) => s" AND (value -> 'value' -> '$langtag')::json IS NOT NULL"
      case _ => ""
    }

    connection.selectSingleValue[Boolean](
      s"SELECT COUNT(*) > 0 FROM user_table_history_$tableId WHERE column_id = ? AND row_id = ? $whereLanguage",
      Json.arr(columnId, rowId))
  }

  private def retrieveCellValue(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      langtagCountryOpt: Option[String] = None
  ): Future[Option[Any]] = {
    for {
      cell <- tableauxModel.retrieveCell(table, column.id, rowId)
    } yield {
      Option(cell.value) match {
        case Some(v) =>
          column match {
            case MultiLanguageColumn(_) =>
              val rawValue = cell.getJson.getJsonObject("value")
              column match {
                case _: BooleanColumn => Option(rawValue.getBoolean(langtagCountryOpt.getOrElse(""), false))
                case _ => Option(rawValue.getValue(langtagCountryOpt.getOrElse("")))
              }
            case _: SimpleValueColumn[_] => Some(v)
            case _: AttachmentColumn => ???
          }
        case _ => None
      }
    }
  }
}

case class CreateHistoryModel(
    protected[this] val tableauxModel: TableauxModel,
    protected[this] val connection: DatabaseConnection
) extends DatabaseQuery
    with CreateHistoryModelBase {

  def create(
      table: Table,
      rowId: RowId,
      values: Seq[(ColumnType[_], _)],
      replace: Boolean = false
  ): Future[Unit] = {

    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, links, replace)
//          _ <- if (attachments.isEmpty) Future.successful(()) else createSimple(table, columnId, rowId, simple)
        } yield ()
    }
  }

  def deleteLinks(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])]
  ): Future[Unit] = {
    val futureSequenceLinksToPut = links.map({
      case (col, linkIdsToDelete) => {
        for {
          currentLinkIds <- retrieveCurrentLinkIds(table, col, rowId)
          linkIds = currentLinkIds.diff(linkIdsToDelete)
        } yield (col, linkIds)
      }
    })

    for {
      linksToPut <- Future.sequence(futureSequenceLinksToPut)
    } yield {
      // For deleting links, we pretend to put a new sequence of links
      createLinks(table, rowId, linksToPut, true)
    }
  }

  def updateLinkOrder(
      table: Table,
      column: LinkColumn,
      rowId: RowId,
  ): Future[Unit] = {

    for {
      linkIds <- retrieveCurrentLinkIds(table, column, rowId)
    } yield {
      // For updating links ordering, we pretend to put a new sequence of links
      createLinks(table, rowId, Seq((column, linkIds)), true)
    }
  }
}
