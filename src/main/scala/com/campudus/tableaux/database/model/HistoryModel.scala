package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.InvalidRequestException
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering, RowId, TableId}
import com.campudus.tableaux.database.model.structure.TableModel
import com.campudus.tableaux.helper.IdentifierFlattener
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{JsonObject, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieveCell(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      langtagOpt: Option[String],
      eventOpt: Option[String]
  ): Future[SeqCellHistory] = {

    val whereLanguage = (column.languageType, langtagOpt) match {
      case (MultiLanguage, Some(langtag)) => s" AND (value -> 'value' -> '$langtag')::json IS NOT NULL"
      case (_, None) => ""
      case (_, Some(_)) =>
        throw new InvalidRequestException(
          "History values filtered by langtags can only be retrieved from multi-language columns")
    }

    val binds = eventOpt match {
      case Some(event) => List(rowId, column.id, event)
      case None => List(rowId, column.id)
    }

    val whereEvent = eventOpt match {
      case Some(_) => s" AND event = ?"
      case None => ""
    }

    val select =
      s"""
         |  SELECT
         |    revision,
         |    row_id,
         |    column_id,
         |    event,
         |    type,
         |    language_type,
         |    author,
         |    timestamp,
         |    value
         |  FROM
         |    user_table_history_${table.id}
         |  WHERE
         |    row_id = ?
         |    AND (column_id = ? OR column_id IS NULL)
         |    $whereLanguage
         |    $whereEvent
         |  ORDER BY
         |    timestamp ASC
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(binds: _*))
    } yield {
      val cellHistory = resultObjectToJsonArray(result).map(mapToCellHistory)
      SeqCellHistory(cellHistory)
    }
  }

  def retrieveTable(
      table: Table,
      eventOpt: Option[String]
  ): Future[SeqCellHistory] = {

    val binds = eventOpt match {
      case Some(event) => List(event)
      case None => List.empty[String]
    }

    val whereEvent = eventOpt match {
      case Some(_) => s" AND event = ?"
      case None => ""
    }

    val select =
      s"""
         |  SELECT
         |    revision,
         |    row_id,
         |    column_id,
         |    event,
         |    type,
         |    language_type,
         |    author,
         |    timestamp,
         |    value
         |  FROM
         |    user_table_history_${table.id}
         |  WHERE
         |    1 = 1
         |    $whereEvent
         |  ORDER BY
         |    timestamp ASC
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(binds: _*))
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
      row.getLong(1),
      row.getLong(2),
      row.getString(3),
      row.getString(4),
      LanguageType(Option(row.getString(5))),
      row.getString(6),
      convertStringToDateTime(row.getString(7)),
      parseJson(row.getString(8))
    )
  }
}

sealed trait CreateHistoryModelBase extends DatabaseQuery {
  protected[this] val tableauxModel: TableauxModel
  protected[this] val connection: DatabaseConnection

  val tableModel = new TableModel(connection)
  val attachmentModel = AttachmentModel(connection)

  protected def retrieveCurrentLinkIds(
      table: Table,
      column: LinkColumn,
      rowId: RowId
  ): Future[Seq[RowId]] = {
    for {
      cell <- tableauxModel.retrieveCell(table, column.id, rowId)
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
      bidirectionalInsert: Boolean = false
  ): Future[Seq[Any]] = {

    def wrapValue(valueSeq: Seq[(_, RowId, Object)]): JsonObject = {
      Json.obj(
        "value" ->
          valueSeq.map({
            case (_, rowId, value) => Json.obj("id" -> rowId, "value" -> value)
          })
      )
    }

    Future.sequence(
      links.map({
        case (column, linkIdsToPutOrAdd) => {
          if (linkIdsToPutOrAdd.isEmpty) {
            insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapValue(Seq()))
          } else {
            for {
              linkIds <- retrieveCurrentLinkIds(table, column, rowId)
              identifierCellSeq <- retrieveForeignIdentifierCells(column, linkIds)
              langTags <- tableModel.retrieveGlobalLangtags()

//               dependentColumns <- tableauxModel.retrieveDependencies(table)
//               _ <- Future.sequence(dependentColumns.map({
//                 case DependentColumnInformation(tableId, columnId, _, _, _) => {
//                   for {
//                     table <- tableModel.retrieve(tableId)
//                     column <- tableauxModel.retrieveColumn(table, columnId)
//                     _ <- createLinks(table, column.linkId, Seq((column.asInstanceOf[LinkColumn], Seq())), false, true)
//                   } yield {}
//                   Future.successful(())
//                 }
//
//               }))

            } yield {
              val preparedData = identifierCellSeq.map(cell => {
                IdentifierFlattener.compress(langTags, cell.value) match {
                  case Right(m) => (MultiLanguage, cell.rowId, m)
                  case Left(s) => (LanguageNeutral, cell.rowId, s)
                }
              })

              // Fallback for languageType if there is no link (case for clear cell)
              val languageType = Try(preparedData.head._1).getOrElse(LanguageNeutral)
              insertCellHistory(table, rowId, column.id, column.kind, languageType, wrapValue(preparedData))
            }
          }
        }
      })
    )
  }

  private def retrieveForeignIdentifierCells(
      column: LinkColumn,
      linkIds: Seq[RowId]
  ): Future[Seq[Cell[Any]]] = {
    val linkedCellSeq = linkIds.map(
      linkId =>
        for {
          foreignIdentifier <- tableauxModel.retrieveCell(column.to.table, column.to.id, linkId)
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
              insertCellHistory(table, rowId, columnId, columnType, languageType, value)
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
          insertCellHistory(table, rowId, columnId, columnType, languageType, value)
      })

    Future.sequence(futureSequence)
  }

  protected def createAttachments(
      table: Table,
      rowId: RowId,
      values: Seq[(AttachmentColumn, Seq[(UUID, Option[Ordering])])]
  ): Future[_] = {

    def wrapValue(attachments: Seq[AttachmentFile]): JsonObject = {
      Json.obj("value" -> attachments.map(_.getJson))
    }

    val futureSequence = values.map({
      case (column: AttachmentColumn, _) => {
        for {
          files <- attachmentModel.retrieveAll(table.id, column.id, rowId)
        } yield {
          insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapValue(files))
        }
      }
    })

    Future.sequence(futureSequence)
  }

  private def insertHistory(
      tableId: TableId,
      rowId: RowId,
      columnIdOpt: Option[ColumnId],
      eventType: String,
      historyTypeOpt: Option[String],
      languageTypeOpt: Option[String],
      jsonStringOpt: Option[String]
  ): Future[RowId] = {
    for {
      result <- connection.query(
        s"""INSERT INTO
           |  user_table_history_$tableId
           |    (row_id, column_id, event, type, language_type, value)
           |  VALUES
           |    (?, ?, ?, ?, ?, ?)
           |  RETURNING revision""".stripMargin,
        Json.arr(rowId,
                 columnIdOpt.getOrElse(null),
                 eventType,
                 historyTypeOpt.getOrElse(null),
                 languageTypeOpt.getOrElse(null),
                 jsonStringOpt.getOrElse(null))
      )
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  private def insertCellHistory(
      table: Table,
      rowId: RowId,
      columnId: ColumnId,
      columnType: TableauxDbType,
      languageType: LanguageType,
      json: JsonObject
  ): Future[RowId] = {
    logger.info(s"createCellHistory ${table.id} $columnId $rowId $json")
    insertHistory(table.id,
                  rowId,
                  Some(columnId),
                  CellChangedEvent.toString,
                  Some(columnType.toString),
                  Some(languageType.toString),
                  Some(json.toString))
  }

  protected def insertRowHistory(
      table: Table,
      rowId: RowId,
  ): Future[RowId] = {
    logger.info(s"createRowHistory ${table.id} $rowId")
    insertHistory(table.id, rowId, None, RowCreatedEvent.toString, None, None, None)
  }

  protected def insertAnnotationHistory(
      table: Table,
      rowId: RowId,
      columnId: ColumnId,
      event: HistoryEventType,
      annotationType: CellAnnotationType,
      languageType: LanguageType,
      json: JsonObject
  ): Future[RowId] = {
    insertHistory(table.id,
                  rowId,
                  Some(columnId),
                  event.toString,
                  Some(annotationType.toString),
                  Some(languageType.toString),
                  Some(json.toString))
  }
}

case class CreateInitialHistoryModel(
    override val tableauxModel: TableauxModel,
    override val connection: DatabaseConnection
) extends DatabaseQuery
    with CreateHistoryModelBase {

  def createIfNotExists(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, links)
          _ <- if (attachments.isEmpty) Future.successful(()) else createAttachmentsInit(table, rowId, attachments)
        } yield ()
    }
  }

  def createClearCellIfNotExists(table: Table, rowId: RowId, columns: Seq[ColumnType[_]]): Future[Unit] = {
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    val simplesWithEmptyValues = for {
      column <- simples
    } yield (column, None)

    val multisWithEmptyValues = for {
      column <- multis
      langtag <- table.langtags match {
        case Some(value) => value
        case _ => Seq.empty[String]
      }
    } yield (column, Map(langtag -> None))

    val linksWithEmptyValues = for {
      column <- links
    } yield (column, Seq.empty[RowId])

    val attachmentsWithEmptyValues = for {
      column <- attachments
    } yield (column, Seq())

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simplesWithEmptyValues)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, multisWithEmptyValues)
      _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, linksWithEmptyValues)
      _ <- if (attachments.isEmpty) Future.successful(())
      else createAttachmentsInit(table, rowId, attachmentsWithEmptyValues)
    } yield ()
  }

  def createAttachmentsInit(
      table: Table,
      rowId: RowId,
      values: Seq[(AttachmentColumn, Seq[(UUID, Option[Ordering])])]
  ): Future[Seq[Unit]] = {

    Future.sequence(values.map({
      case (column, _) => {
        for {
          historyEntryExists <- historyExists(table.id, column.id, rowId)
          _ <- if (!historyEntryExists) {
            for {
              currentAttachments <- attachmentModel.retrieveAll(table.id, column.id, rowId)
              _ <- if (currentAttachments.nonEmpty)
                createAttachments(table, rowId, values)
              else
                Future.successful(())
            } yield ()
          } else
            Future.successful(())
        } yield ()
      }
    }))
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

  // TODO refactor init methods
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
          }
        case _ => None
      }
    }
  }
}

case class CreateHistoryModel(
    override val tableauxModel: TableauxModel,
    override val connection: DatabaseConnection
) extends DatabaseQuery
    with CreateHistoryModelBase {

  def removeCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      uuid: UUID,
      annotation: CellLevelAnnotation
  ): Future[Unit] = {
    val languagetype = if (annotation.langtags.isEmpty) LanguageNeutral else MultiLanguage

    def wrapValue(value: String, uuid: UUID): JsonObject = {
      Json.obj(
        "value" -> value,
        "uuid" -> uuid.toString
      )
    }

    logger.info(s"createRemoveAnnotationHistory ${column.table.id} $rowId ${annotation.value}")
    for {
      _ <- insertAnnotationHistory(column.table,
                                   rowId,
                                   column.id,
                                   AnnotationRemovedEvent,
                                   annotation.annotationType,
                                   languagetype,
                                   wrapValue(annotation.value, uuid))
    } yield ()
  }

  def addCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      uuid: UUID,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  ): Future[Seq[RowId]] = {
    val languagetype = if (langtags.isEmpty) LanguageNeutral else MultiLanguage

    def wrapValue(value: String, uuid: UUID): JsonObject = Json.obj("value" -> value, "uuid" -> uuid.toString)
    def wrapLanguageValue(langtag: String, value: String, uuid: UUID): JsonObject =
      Json.obj("value" -> Json.obj(langtag -> value), "uuid" -> uuid.toString)

    logger.info(s"createAddAnnotationHistory ${column.table.id} $rowId $langtags $value")

    val futureSequence: Seq[Future[RowId]] = languagetype match {
      case LanguageNeutral =>
        Seq(for {
          historyRowId <- insertAnnotationHistory(column.table,
                                                  rowId,
                                                  column.id,
                                                  AnnotationAddedEvent,
                                                  annotationType,
                                                  languagetype,
                                                  wrapValue(value, uuid))
        } yield historyRowId)
      case MultiLanguage => {
        langtags.map(langtag =>
    for {
            historyRowId <- insertAnnotationHistory(column.table,
                                   rowId,
                                   column.id,
                                   AnnotationAddedEvent,
                                   annotationType,
                                   languagetype,
                                                    wrapLanguageValue(langtag, value, uuid))
          } yield historyRowId)
      }
      case MultiCountry(_) => Seq.empty[Future[RowId]]
    }

    Future.sequence(futureSequence)
  }

  def createClearCell(table: Table, rowId: RowId, columns: Seq[ColumnType[_]]): Future[Unit] = {
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    val simplesWithEmptyValues = for {
      column <- simples
    } yield (column, None)

    val multisWithEmptyValues = for {
      column <- multis
      langtag <- table.langtags match {
        case Some(value) => value
        case _ => Seq.empty[String]
      }
    } yield (column, Map(langtag -> None))

    val linksWithEmptyValues = for {
      column <- links
    } yield (column, Seq.empty[RowId])

    val attachmentsWithEmptyValues = for {
      column <- attachments
    } yield (column, Seq())

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simplesWithEmptyValues)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multisWithEmptyValues)
      _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, linksWithEmptyValues)
      _ <- if (attachments.isEmpty) Future.successful(())
      else createAttachments(table, rowId, attachmentsWithEmptyValues)
    } yield ()
  }

  def createCells(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, links)
          _ <- if (attachments.isEmpty) Future.successful(()) else createAttachments(table, rowId, attachments)
        } yield ()
    }
  }

  def createRow(table: Table, rowId: RowId): Future[RowId] = {
    insertRowHistory(table, rowId)
  }
}
