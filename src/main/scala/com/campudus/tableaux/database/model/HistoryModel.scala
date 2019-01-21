package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.RequestContext
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering, RowId, TableId}
import com.campudus.tableaux.database.model.structure.TableModel
import com.campudus.tableaux.helper.IdentifierFlattener
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{JsonObject, _}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieveCell(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): Future[SeqHistory] = {
    val (where, binds) = generateWhereAndBinds(Some(column.id), Some(rowId), langtagOpt, typeOpt)
    retrieve(table, where, binds)
  }

  def retrieveRow(
      table: Table,
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): Future[SeqHistory] = {
    val (where, binds) = generateWhereAndBinds(None, Some(rowId), langtagOpt, typeOpt)
    retrieve(table, where, binds)
  }

  def retrieveTable(
      table: Table,
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): Future[SeqHistory] = {
    val (where, binds) = generateWhereAndBinds(None, None, langtagOpt, typeOpt)
    retrieve(table, where, binds)
  }

  private def mapToHistory(row: JsonArray): History = {

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

    History(
      row.getLong(0),
      row.getLong(1),
      row.getLong(2),
      row.getString(3),
      HistoryType(row.getString(4)),
      row.getString(5),
      LanguageType(Option(row.getString(6))),
      row.getString(7),
      convertStringToDateTime(row.getString(8)),
      parseJson(row.getString(9))
    )
  }

  private def retrieve(table: Table, where: String, binds: Seq[Any]): Future[SeqHistory] = {
    val select =
      s"""
         |  SELECT
         |    revision,
         |    row_id,
         |    column_id,
         |    event,
         |    history_type,
         |    value_type,
         |    language_type,
         |    author,
         |    timestamp,
         |    value
         |  FROM
         |    user_table_history_${table.id}
         |  WHERE
         |    1 = 1
         |    $where
         |  ORDER BY
         |    revision ASC
         """.stripMargin
    // order by must base on revision, because a lower revision could have a minimal later timestamp

    for {
      result <- connection.query(select, Json.arr(binds: _*))
    } yield {
      val histories = resultObjectToJsonArray(result).map(mapToHistory)
      SeqHistory(histories)
    }
  }

  private def generateWhereAndBinds(
      columnIdOpt: Option[ColumnId],
      rowIdOpt: Option[RowId],
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): (String, Seq[Any]) = {

    val whereColumnId: Option[(String, ColumnId)] = columnIdOpt match {
      case Some(columnId) => Some(s" AND (column_id = ? OR column_id IS NULL)", columnId)
      case None => None
    }

    val whereRowId: Option[(String, RowId)] = rowIdOpt match {
      case Some(rowId) => Some(s" AND row_id = ?", rowId)
      case None => None
    }

    val whereLangtag: Option[(String, String)] = langtagOpt match {
//      case Some(langtag) => Some(s" AND (value -> 'value' -> ?)::json IS NOT NULL", langtag)
      case Some(langtag) =>
        Some(
          s"""
             |AND (
             |  language_type != 'language' OR
             |  language_type IS NULL OR
             |  (language_type = 'language' AND (value -> 'value' -> ?)::json IS NOT NULL)
             |)
         """.stripMargin,
          langtag
        )
      case None => None
    }

    val whereType: Option[(String, String)] = typeOpt match {
      case Some(historyType) => Some(s" AND history_type = ?", historyType)
      case None => None
    }

    val opts: List[Option[(String, Any)]] = List(whereColumnId, whereRowId, whereLangtag, whereType)

    opts.foldLeft(("", Seq.empty[Any])) { (acc, op) =>
      {
        val (foldWhere, foldBind) = acc
        op match {
          case Some((where, bind: Any)) => (s"$foldWhere $where", foldBind :+ bind)
          case None => acc
        }
      }
    }
  }
}

sealed trait CreateHistoryModelBase extends DatabaseQuery {
  implicit val requestContext: RequestContext
  protected[this] val tableauxModel: TableauxModel
  protected[this] val connection: DatabaseConnection

  val tableModel = new TableModel(connection)
  val attachmentModel = AttachmentModel(connection)

  protected def getUserName: String = {
    requestContext.getCookieValue("userName")
  }

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

  protected def wrapLinkValue(valueSeq: Seq[(_, RowId, Object)]): JsonObject = {
    Json.obj(
      "value" ->
        valueSeq.map({
          case (_, rowId, value) => Json.obj("id" -> rowId, "value" -> value)
        })
    )
  }

  protected def createLinks(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])],
      allowRecursion: Boolean = true
  ): Future[Seq[Any]] = {

    def isNotDeleteCascade(column: LinkColumn): Boolean = {
      !column.linkDirection.constraint.deleteCascade
    }

    Future.sequence(
      links.map({
        case (column, linkIdsToPutOrAdd) => {
          if (linkIdsToPutOrAdd.isEmpty) {
            insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapLinkValue(Seq()))
          } else {
            for {
              linkIds <- retrieveCurrentLinkIds(table, column, rowId)
              identifierCellSeq <- retrieveForeignIdentifierCells(column, linkIds)
              langTags <- getLangTags(table)

              dependentColumns <- tableauxModel.retrieveDependencies(table)

              preparedData = identifierCellSeq.map(cell => {
                IdentifierFlattener.compress(langTags, cell.value) match {
                  case Right(m) => (MultiLanguage, cell.rowId, m)
                  case Left(s) => (LanguageNeutral, cell.rowId, s)
                }
              })

              // Fallback for languageType if there is no link (case for clear cell)
              languageType = Try(preparedData.head._1).getOrElse(LanguageNeutral)

              _ <- insertCellHistory(table, rowId, column.id, column.kind, languageType, wrapLinkValue(preparedData))

              _ <- if (allowRecursion && isNotDeleteCascade(column)) {
                Future.sequence(dependentColumns.map({
                  case DependentColumnInformation(linkedTableId, linkedColumnId, _, _, _) => {
                    for {
                      linkedTable <- tableModel.retrieve(linkedTableId)
                      linkedColumn <- tableauxModel.retrieveColumn(linkedTable, linkedColumnId)
                      _ <- Future.sequence(linkIdsToPutOrAdd.map(linkId => {
                        for {
                          // invalidate dependent columns from backlinks point of view
                          _ <- tableauxModel.invalidateCellAndDependentColumns(linkedColumn, linkId)
                          _ <- createLinks(linkedTable,
                                           linkId,
                                           Seq((linkedColumn.asInstanceOf[LinkColumn], Seq(linkId))),
                                           false)
                        } yield ()
                      }))
                    } yield ()
                  }
                }))
              } else {
                Future.successful(())
              }
            } yield ()
          }
        }
      })
    )
  }

  def updateLinkOrder(
      table: Table,
      linkColumn: LinkColumn,
      rowId: RowId
  ): Future[Unit] = {

    for {
      linkIds <- retrieveCurrentLinkIds(table, linkColumn, rowId)
      identifierCellSeq <- retrieveForeignIdentifierCells(linkColumn, linkIds)
      langTags <- getLangTags(table)

      preparedData = identifierCellSeq.map(cell => {
        IdentifierFlattener.compress(langTags, cell.value) match {
          case Right(m) => (MultiLanguage, cell.rowId, m)
          case Left(s) => (LanguageNeutral, cell.rowId, s)
        }
      })
      _ <- insertCellHistory(table,
                             rowId,
                             linkColumn.id,
                             linkColumn.kind,
                             preparedData.head._1,
                             wrapLinkValue(preparedData))
    } yield ()
  }

  def getLangTags(table: Table): Future[Seq[String]] = {
    table.langtags match {
      case Some(langtags) => Future.successful(langtags)
      case None => tableModel.retrieveGlobalLangtags()
    }
  }

  protected def retrieveForeignIdentifierCells(
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
          _ <- insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapValue(files))
        } yield ()
      }
    })

    Future.sequence(futureSequence)
  }

  protected def insertHistory(
      tableId: TableId,
      rowId: RowId,
      columnIdOpt: Option[ColumnId],
      eventType: HistoryEventType,
      historyType: HistoryType,
      valueTypeOpt: Option[String],
      languageTypeOpt: Option[String],
      jsonStringOpt: Option[String],
      userName: String
  ): Future[RowId] = {
    for {
      result <- connection.query(
        s"""INSERT INTO
           |  user_table_history_$tableId
           |    (row_id, column_id, event, history_type, value_type, language_type, value, author)
           |  VALUES
           |    (?, ?, ?, ?, ?, ?, ?, ?)
           |  RETURNING revision""".stripMargin,
        Json.arr(
          rowId,
          columnIdOpt.getOrElse(null),
          eventType.toString,
          historyType.toString,
          valueTypeOpt.getOrElse(null),
          languageTypeOpt.getOrElse(null),
          jsonStringOpt.getOrElse(null),
          userName
        )
      )
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }

  protected def insertCellHistory(
      table: Table,
      rowId: RowId,
      columnId: ColumnId,
      columnType: TableauxDbType,
      languageType: LanguageType,
      json: JsonObject
  ): Future[RowId] = {
    logger.info(s"createCellHistory ${table.id} $columnId $rowId $json $getUserName")
    insertHistory(
      table.id,
      rowId,
      Some(columnId),
      CellChangedEvent,
      HistoryTypeCell,
      Some(columnType.toString),
      Some(languageType.toString),
      Some(json.toString),
      getUserName
    )
  }

  protected def insertRowHistory(
      table: Table,
      rowId: RowId
  ): Future[RowId] = {
    logger.info(s"createRowHistory ${table.id} $rowId $getUserName")
    insertHistory(table.id, rowId, None, RowCreatedEvent, HistoryTypeRow, None, None, None, getUserName)
  }

  protected def addRowAnnotationHistory(
      tableId: TableId,
      rowId: RowId,
      value: String
  ): Future[RowId] = {
    logger.info(s"createAddRowAnnotationHistory $tableId $rowId $getUserName")
    val json = Json.obj("value" -> value)
    insertHistory(tableId,
                  rowId,
                  None,
                  AnnotationAddedEvent,
                  HistoryTypeRowFlag,
                  Some(value),
                  Some(LanguageType.NEUTRAL),
                  Some(json.toString),
                  getUserName)
  }

  protected def removeRowAnnotationHistory(
      tableId: TableId,
      rowId: RowId,
      value: String
  ): Future[RowId] = {
    logger.info(s"createRemoveRowAnnotationHistory $tableId $rowId $getUserName")
    val json = Json.obj("value" -> value)
    insertHistory(tableId,
                  rowId,
                  None,
                  AnnotationRemovedEvent,
                  HistoryTypeRowFlag,
                  Some(value),
                  Some(LanguageType.NEUTRAL),
                  Some(json.toString),
                  getUserName)
  }
}

case class CreateInitialHistoryModel(
    override val tableauxModel: TableauxModel,
    override val connection: DatabaseConnection
)(implicit val requestContext: RequestContext)
    extends DatabaseQuery
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
      case (column, _) =>
        for {
          historyEntryExists <- historyExists(table.id, column.id, rowId)
          _ <- if (!historyEntryExists) {
            for {
              currentAttachments <- attachmentModel.retrieveAll(table.id, column.id, rowId)
              _ <- if (currentAttachments.nonEmpty)
                createAttachments(table, rowId, values)
              else {
                Future.successful(())
              }
            } yield ()
          } else
            Future.successful(())
        } yield ()
    }))
  }

  def createLinksInit(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])],
  ): Future[Seq[Unit]] = {

    Future.sequence(links.map({
      case (column, _) =>
        for {
          historyEntryExists <- historyExists(table.id, column.id, rowId)
          _ <- if (!historyEntryExists) {
            for {
              linkIds <- retrieveCurrentLinkIds(table, column, rowId)
              _ <- if (linkIds.nonEmpty)
                createLinks(table, rowId, Seq((column, linkIds)), allowRecursion = false)
              else
                Future.successful(())
            } yield ()
          } else
            Future.successful(())
        } yield ()
    }))
  }

  private def createSimpleInit(
      table: Table,
      rowId: RowId,
      simples: Seq[(SimpleValueColumn[_], Option[Any])]
  ): Future[Seq[Unit]] = {

    def createIfNotEmpty(column: SimpleValueColumn[_]): Future[Unit] = {
      for {
        value <- retrieveCellValue(table, column, rowId)
        _ <- value match {
          case Some(v) => createSimple(table, rowId, Seq((column, Option(v))))
          case None => Future.successful(())
        }
      } yield ()
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

    def createIfNotEmpty(column: SimpleValueColumn[_], langtag: String): Future[Unit] = {
      for {
        value <- retrieveCellValue(table, column, rowId, Option(langtag))
        _ <- value match {
          case Some(v) => createTranslation(table, rowId, Seq((column, Map(langtag -> Option(v)))))
          case None => Future.successful(())
        }
      } yield ()
    }

    Future.sequence(
      columnsForLang.toSeq
        .flatMap({
          case (langtag, columnValueOptSeq) =>
            columnValueOptSeq
              .map({
                case (column: SimpleValueColumn[_], _) =>
                  for {
                    historyEntryExists <- historyExists(table.id, column.id, rowId, Option(langtag))
                    _ <- if (!historyEntryExists) createIfNotEmpty(column, langtag) else Future.successful(())
                  } yield ()
              })
        }))
  }

  private def historyExists(
      tableId: TableId,
      columnId: ColumnId,
      rowId: RowId,
      langtagOpt: Option[String] = None
  ): Future[Boolean] = {

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
)(implicit val requestContext: RequestContext)
    extends DatabaseQuery
    with CreateHistoryModelBase {

  def wrapAnnotationValue(value: String, uuid: UUID, langtagOpt: Option[String] = None): JsonObject = {
    langtagOpt match {
      case Some(langtag) => Json.obj("value" -> Json.obj(langtag -> value), "uuid" -> uuid.toString)
      case None => Json.obj("value" -> value, "uuid" -> uuid.toString)
    }
  }

  def removeCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      uuid: UUID,
      annotation: CellLevelAnnotation,
      langtagOpt: Option[String] = None
  ): Future[Seq[RowId]] = {
    val languageType = if (annotation.langtags.isEmpty) LanguageNeutral else MultiLanguage

    val annotationsToBeRemoved = langtagOpt match {
      case Some(langtag) => Seq(langtag)
      case None => annotation.langtags
    }

    logger.info(
      s"createRemoveAnnotationHistory ${column.table.id} $rowId ${annotation.annotationType} ${annotation.value} $getUserName")

    insertAnnotationHistory(column.table,
                            column,
                            rowId,
                            uuid,
                            AnnotationRemovedEvent,
                            annotationsToBeRemoved,
                            annotation.annotationType,
                            annotation.value,
                            languageType,
                            getUserName)
  }

  def addCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      uuid: UUID,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  ): Future[Seq[RowId]] = {
    val languageType = if (langtags.isEmpty) LanguageNeutral else MultiLanguage

    logger.info(s"createAddAnnotationHistory ${column.table.id} $rowId $langtags $value $getUserName")

    insertAnnotationHistory(column.table,
                            column,
                            rowId,
                            uuid,
                            AnnotationAddedEvent,
                            langtags,
                            annotationType,
                            value,
                            languageType,
                            getUserName)
  }

  private def insertAnnotationHistory(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      uuid: UUID,
      eventType: HistoryEventType,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String,
      languageType: LanguageType,
      userName: String
  ): Future[Seq[RowId]] = {
    val futureSequence: Seq[Future[RowId]] = (annotationType, languageType) match {
      // All comment annotation types are of languageType LanguageNeutral
      case (InfoAnnotationType | WarningAnnotationType | ErrorAnnotationType, _) =>
        Seq(for {
          historyRowId <- insertHistory(
            table.id,
            rowId,
            Some(column.id),
            eventType,
            HistoryTypeCellComment,
            Some(annotationType.toString),
            Some(languageType.toString),
            Some(wrapAnnotationValue(value, uuid).toString),
            userName
          )
        } yield historyRowId)
      case (_, LanguageNeutral) =>
        Seq(for {
          historyRowId <- insertHistory(
            table.id,
            rowId,
            Some(column.id),
            eventType,
            HistoryTypeCellFlag,
            Some(value),
            Some(languageType.toString),
            Some(wrapAnnotationValue(value, uuid).toString),
            userName
          )
        } yield historyRowId)
      case (_, MultiLanguage) =>
        langtags.map(langtag =>
          for {
            historyRowId <- insertHistory(
              table.id,
              rowId,
              Some(column.id),
              eventType,
              HistoryTypeCellFlag,
              Some(value),
              Some(languageType.toString),
              Some(wrapAnnotationValue(value, uuid, Some(langtag)).toString),
              userName
            )
          } yield historyRowId)
      case (_, MultiCountry(_)) => Seq.empty[Future[RowId]]
    }

    Future.sequence(futureSequence)
  }

  def createClearCell(table: Table, rowId: RowId, columns: Seq[ColumnType[_]]): Future[Unit] = {
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    val simplesWithEmptyValues = simples.map(column => (column, None))

    val multisWithEmptyValues = for {
      column <- multis
      langtag <- table.langtags match {
        case Some(value) => value
        case _ => Seq.empty[String]
      }
    } yield (column, Map(langtag -> None))

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simplesWithEmptyValues)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multisWithEmptyValues)
      _ <- if (links.isEmpty) Future.successful(()) else createClearLink(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(())
      else clearAttachments(table, rowId, attachments)
    } yield ()
  }

  protected def clearAttachments(
      table: Table,
      rowId: RowId,
      columns: Seq[AttachmentColumn]
  ): Future[_] = {

    val futureSequence = columns.map(
      column =>
        insertCellHistory(table,
                          rowId,
                          column.id,
                          column.kind,
                          column.languageType,
                          Json.obj("value" -> Json.emptyArr())))

    Future.sequence(futureSequence)
  }

  def createCells(
      table: Table,
      rowId: RowId,
      values: Seq[(ColumnType[_], _)],
      allowRecursion: Boolean = true
  ): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, links, allowRecursion)
          _ <- if (attachments.isEmpty) Future.successful(()) else createAttachments(table, rowId, attachments)
        } yield ()
    }
  }

  def createRow(table: Table, rowId: RowId): Future[RowId] = {
    insertRowHistory(table, rowId)
  }

  def updateRowsAnnotation(tableId: TableId, rowIds: Seq[RowId], finalFlagOpt: Option[Boolean]): Future[Unit] = {
    for {
      _ <- finalFlagOpt match {
        case None => Future.successful(())
        case Some(isFinal) =>
          if (isFinal)
            Future.sequence(rowIds.map(rowId => addRowAnnotationHistory(tableId, rowId, "final")))
          else
            Future.sequence(rowIds.map(rowId => removeRowAnnotationHistory(tableId, rowId, "final")))
      }
    } yield ()
  }

  def createClearLink(
      table: Table,
      rowId: RowId,
      columns: Seq[LinkColumn]
  ): Future[Unit] = {
    for {
      _ <- createLinks(table, rowId, columns.map(column => (column, Seq.empty[RowId])), false)
      _ <- Future.sequence(
        columns.map({ column =>
          {
            for {
              dependentColumns <- tableauxModel.retrieveDependencies(table)
              foreignIds <- tableauxModel.updateRowModel.retrieveLinkedRows(table, rowId, column)
              _ <- Future.sequence(dependentColumns.map({
                case DependentColumnInformation(linkedTableId, linkedColumnId, _, _, _) => {
                  for {
                    linkedTable <- tableModel.retrieve(linkedTableId)
                    linkedColumn <- tableauxModel.retrieveColumn(linkedTable, linkedColumnId)
                    _ <- Future.sequence(foreignIds.map(linkId => {
                      for {
                        _ <- insertCellHistory(linkedTable,
                                               linkId,
                                               linkedColumn.id,
                                               linkedColumn.kind,
                                               linkedColumn.languageType,
                                               wrapLinkValue(Seq()))
                      } yield ()
                    }))
                  } yield ()
                }
              }))
            } yield ()
          }
        })
      )
    } yield ()
  }

  def deleteLink(
      table: Table,
      linkColumn: LinkColumn,
      rowId: RowId,
      toId: RowId
  ): Future[_] = {
    createLinks(table, rowId, Seq((linkColumn, Seq(toId))), true)
  }
}
