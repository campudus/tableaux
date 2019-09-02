package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.RequestContext
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.database.model.structure.TableModel
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.helper.{IdentifierFlattener, JsonUtils}
import com.campudus.tableaux.router.auth.permission.RoleModel
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieveCell(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(Some(column.id), Some(rowId), langtagOpt, typeOpt)
    retrieve(table, where, binds)
  }

  def retrieveRow(
      table: Table,
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(None, Some(rowId), langtagOpt, typeOpt)
    retrieve(table, where, binds)
  }

  def retrieveTable(
      table: Table,
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(None, None, langtagOpt, typeOpt)
    retrieve(table, where, binds)
  }

  private def mapToHistory(row: JsonArray): History = {

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
      JsonUtils.parseJson(row.getString(9))
    )
  }

  private def retrieve(table: Table, where: String, binds: Seq[Any]): Future[Seq[History]] = {
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
      resultObjectToJsonArray(result).map(mapToHistory)
    }
  }

  private def generateWhereAndBinds(
      columnIdOpt: Option[ColumnId],
      rowIdOpt: Option[RowId],
      langtagOpt: Option[String],
      typeOpt: Option[String]
  ): (String, Seq[Any]) = {
    val whereColumnId: Option[(String, ColumnId)] = columnIdOpt.map((s" AND (column_id = ? OR column_id IS NULL)", _))
    val whereRowId: Option[(String, RowId)] = rowIdOpt.map((s" AND row_id = ?", _))

    val whereLangtag: Option[(String, String)] = langtagOpt.map(
      (
        s"""
           |AND (
           |  language_type != 'language' OR
           |  language_type IS NULL OR
           |  (language_type = 'language' AND (value -> 'value' -> ?)::json IS NOT NULL)
           |)
         """.stripMargin,
        _
      ))

    val whereType: Option[(String, String)] = typeOpt.map((s" AND history_type = ?", _))

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

case class CreateHistoryModel(tableauxModel: TableauxModel, connection: DatabaseConnection)(
    implicit requestContext: RequestContext,
    roleModel: RoleModel
) extends DatabaseQuery {

  val tableModel = new TableModel(connection)
  val attachmentModel = AttachmentModel(connection)

  private def getUserName: String = {
    requestContext.getPrincipleString("preferred_username", "dev")
  }

  private def retrieveCurrentLinkIds(table: Table, column: LinkColumn, rowId: RowId): Future[Seq[RowId]] = {
    for {
      cell <- tableauxModel.retrieveCell(table, column.id, rowId, isInternalCall = true)
    } yield {
      cell.getJson
        .getJsonArray("value")
        .asScala
        .map(_.asInstanceOf[JsonObject])
        .map(_.getLong("id").longValue())
        .toSeq
    }
  }

  private def getLinksData(idCellSeq: Seq[Cell[Any]], langTags: Seq[String]): (LanguageType, Seq[(RowId, Object)]) = {
    val preparedData = idCellSeq
      .map(cell => {
        IdentifierFlattener.compress(langTags, cell.value) match {
          case Right(m) => (MultiLanguage, cell.rowId, m)
          case Left(s) => (LanguageNeutral, cell.rowId, s)
        }
      })

    val languageType = preparedData.headOption
      .map({ case (lt, _, _) => lt })
      .getOrElse(LanguageNeutral)

    val cellValues = preparedData.map({
      case (_, rowIds, values) => (rowIds, values)
    })

    (languageType, cellValues)
  }

  private def wrapLinkValue(linksData: Seq[(RowId, Object)] = Seq.empty[(RowId, Object)]): JsonObject = {
    Json.obj(
      "value" ->
        linksData.map({
          case (rowId, value) => Json.obj("id" -> rowId, "value" -> value)
        })
    )
  }

  private def createLinks(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])],
      allowRecursion: Boolean
  ): Future[Seq[Any]] = {

    def createLinksRecursive(column: LinkColumn, changedLinkIds: Seq[RowId]): Future[Unit] = {
      for {
        backLinkColumn <- tableauxModel.retrieveBacklink(column)
        _ <- backLinkColumn match {
          case Some(linkColumn) =>
            for {
              _ <- Future.sequence(changedLinkIds.map(linkId => {
                for {
                  // invalidate dependent columns from backlinks point of view
                  _ <- tableauxModel.invalidateCellAndDependentColumns(linkColumn, linkId)
                  _ <- createLinks(column.to.table, linkId, Seq((linkColumn, Seq(linkId))), allowRecursion = false)
                } yield ()
              }))
            } yield ()
          case None => Future.successful(())
        }
      } yield ()
    }

    Future.sequence(
      links.map({
        case (column, changedLinkIds) =>
          if (changedLinkIds.isEmpty) {
            insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapLinkValue())
          } else {
            for {
              linkIds <- retrieveCurrentLinkIds(table, column, rowId)
              identifierCellSeq <- retrieveForeignIdentifierCells(column, linkIds)
              langTags <- getLangTags(table)

              (languageType, linksData) = getLinksData(identifierCellSeq, langTags)
              _ <- insertCellHistory(table, rowId, column.id, column.kind, languageType, wrapLinkValue(linksData))

              _ <- if (allowRecursion) {
                createLinksRecursive(column, changedLinkIds)
              } else {
                Future.successful(())
              }
            } yield ()
          }
      })
    )
  }

  /**
    * Writes a new history entry on base of the current linked rows for a {@code table},
    * a {@code linkColumn} and ({@code rowIds}).
    *
    * Since we want to update the links after deleting dependent rows, we need to check if the rows have not already
    * been deleted by the delete cascade function. For example this happens for back links with "delete cascade" in
    * only one direction.
    *
    * Also make sure the links have changed before and the cache is invalidated.
    *
    * @param table
    * @param linkColumn
    * @param rowIds
    */
  def updateLinks(table: Table, linkColumn: LinkColumn, rowIds: Seq[RowId]): Future[Seq[Unit]] = {
    Future.sequence(
      rowIds.map(
        rowId =>
          for {
            rowExists <- tableauxModel.retrieveRowModel.rowExists(table.id, rowId)

            _ <- if (rowExists) {
              updateLinks(table, linkColumn, rowId, rowExists)
            } else {
              Future.successful(())
            }
          } yield ()
      ))
  }

  private def updateLinks(table: Table, linkColumn: LinkColumn, rowId: RowId, rowExists: Boolean): Future[Unit] = {
    for {
      linkIds <- retrieveCurrentLinkIds(table, linkColumn, rowId)
      identifierCellSeq <- retrieveForeignIdentifierCells(linkColumn, linkIds)
      langTags <- getLangTags(table)
      (languageType, linksData) = getLinksData(identifierCellSeq, langTags)
      _ <- insertCellHistory(table, rowId, linkColumn.id, linkColumn.kind, languageType, wrapLinkValue(linksData))
    } yield ()
  }

  def getLangTags(table: Table): Future[Seq[String]] = {
    table.langtags.map(Future.successful).getOrElse(tableModel.retrieveGlobalLangtags())
  }

  private def retrieveForeignIdentifierCells(column: LinkColumn, linkIds: Seq[RowId]): Future[Seq[Cell[Any]]] = {
    val linkedCellSeq = linkIds.map(
      linkId =>
        for {
          foreignIdentifier <- tableauxModel.retrieveCell(column.to.table, column.to.id, linkId, isInternalCall = true)
        } yield foreignIdentifier
    )
    Future.sequence(linkedCellSeq)
  }

  private def createTranslation(
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
      .toSeq

    Future.sequence(
      columnsForLang
        .flatMap({
          case (langtag, columnValueOptSeq) =>
            columnValueOptSeq
              .map({
                case (column: SimpleValueColumn[_], valueOpt) =>
                  insertCellHistory(table,
                                    rowId,
                                    column.id,
                                    column.kind,
                                    column.languageType,
                                    wrapLanguageValue(langtag, valueOpt.orNull))
              })
        }))
  }

  private def createSimple(
      table: Table,
      rowId: RowId,
      simples: Seq[(SimpleValueColumn[_], Option[Any])]
  ): Future[Seq[RowId]] = {

    def wrapValue(value: Any): JsonObject = Json.obj("value" -> value)

    Future.sequence(
      simples
        .map({
          case (column: SimpleValueColumn[_], valueOpt) =>
            insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapValue(valueOpt.orNull))
        }))
  }

  private def createAttachments(
      table: Table,
      rowId: RowId,
      columns: Seq[AttachmentColumn]
  ): Future[_] = {

    def wrapValue(attachments: Seq[AttachmentFile]): JsonObject = {
      Json.obj("value" -> attachments.map(_.getJson))
    }

    val futureSequence = columns.map({ column =>
      for {
        files <- attachmentModel.retrieveAll(table.id, column.id, rowId)
        _ <- insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapValue(files))
      } yield ()
    })

    Future.sequence(futureSequence)
  }

  private def insertHistory(
      tableId: TableId,
      rowId: RowId,
      columnIdOpt: Option[ColumnId],
      eventType: HistoryEventType,
      historyType: HistoryType,
      valueTypeOpt: Option[String],
      languageTypeOpt: Option[String],
      jsonStringOpt: Option[String]
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
          columnIdOpt.orNull,
          eventType.toString,
          historyType.toString,
          valueTypeOpt.orNull,
          languageTypeOpt.orNull,
          jsonStringOpt.orNull,
          getUserName
        )
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
    logger.info(s"createCellHistory ${table.id} $columnId $rowId $json $getUserName")
    insertHistory(
      table.id,
      rowId,
      Some(columnId),
      CellChangedEvent,
      HistoryTypeCell,
      Some(columnType.toString),
      Some(languageType.toString),
      Some(json.toString)
    )
  }

  private def insertRowHistory(
      table: Table,
      rowId: RowId
  ): Future[RowId] = {
    logger.info(s"createRowHistory ${table.id} $rowId $getUserName")
    insertHistory(table.id, rowId, None, RowCreatedEvent, HistoryTypeRow, None, None, None)
  }

  private def addRowAnnotationHistory(
      tableId: TableId,
      rowId: RowId,
      value: String
  ): Future[RowId] = {
    logger.info(s"createAddRowAnnotationHistory $tableId $rowId $getUserName")
    addOrRemoveAnnotationHistory(tableId, rowId, AnnotationAddedEvent, value)
  }

  private def removeRowAnnotationHistory(
      tableId: TableId,
      rowId: RowId,
      value: String
  ): Future[RowId] = {
    logger.info(s"createRemoveRowAnnotationHistory $tableId $rowId $getUserName")
    addOrRemoveAnnotationHistory(tableId, rowId, AnnotationRemovedEvent, value)
  }

  private def addOrRemoveAnnotationHistory(
      tableId: TableId,
      rowId: RowId,
      eventType: HistoryEventType,
      value: String
  ): Future[RowId] = {
    val json = Json.obj("value" -> value)
    insertHistory(tableId,
                  rowId,
                  None,
                  eventType,
                  HistoryTypeRowFlag,
                  Some(value),
                  Some(LanguageType.NEUTRAL),
                  Some(json.toString))
  }

  /**
    * Creates an initial history cell value when changing a cell value (for backward compatibility).
    *
    * If we didn't call this method on any cell change/deletion the previously
    * valid value wouldn't be logged in a history table.
    */
  def createCellsInit(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[Unit] = {
    val columns = values.map({ case (col: ColumnType[_], _) => col })
    val (simples, _, links, attachments) = ColumnType.splitIntoTypes(columns)

    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((_, multis, _, _)) =>
        // for changes on multi language cells create init values only for langtags to be changed
        val langtagColumns = for {
          (column, langtags) <- multis
          langtagSeq = langtags.toSeq.map({ case (langtag, _) => langtag })
        } yield (langtagSeq, column)

        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, langtagColumns)
          _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, links)
          _ <- if (attachments.isEmpty) Future.successful(()) else createAttachmentsInit(table, rowId, attachments)
        } yield ()
    }
  }

  /**
    * Creates an initial history cell value when deleting a cell value (for backward compatibility).
    *
    * If we didn't call this method on any cell change/deletion the previously
    * valid value wouldn't be logged in a history table.
    */
  def createClearCellInit(table: Table, rowId: RowId, columns: Seq[ColumnType[_]]): Future[Unit] = {
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    // for clearing of multi language cells create init values for all langtags
    val langtagColumns = for {
      column <- multis
      langtag = table.langtags.getOrElse(Seq.empty[String])
    } yield (langtag, column)

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simples)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, langtagColumns)
      _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else createAttachmentsInit(table, rowId, attachments)
    } yield ()
  }

  def createAttachmentsInit(table: Table, rowId: RowId, columns: Seq[AttachmentColumn]): Future[Seq[Unit]] = {
    Future.sequence(columns.map({ column =>
      for {
        historyEntryExists <- historyExists(table.id, column.id, rowId, None)
        _ <- if (!historyEntryExists) {
          for {
            currentAttachments <- attachmentModel.retrieveAll(table.id, column.id, rowId)
            _ <- if (currentAttachments.nonEmpty) {
              createAttachments(table, rowId, Seq(column))
            } else {
              Future.successful(())
            }
          } yield ()
        } else
          Future.successful(())
      } yield ()
    }))
  }

  def createLinksInit(table: Table, rowId: RowId, links: Seq[LinkColumn]): Future[Seq[Unit]] = {

    def createIfNotEmpty(linkColumn: LinkColumn): Future[Unit] = {
      for {
        linkIds <- retrieveCurrentLinkIds(table, linkColumn, rowId)
        _ <- if (linkIds.nonEmpty) {
          createLinks(table, rowId, Seq((linkColumn, linkIds)), allowRecursion = true)
        } else {
          Future.successful(())
        }
      } yield ()
    }

    Future.sequence(links.map({ linkColumn =>
      for {
        historyEntryExists <- historyExists(table.id, linkColumn.id, rowId, None)
        _ <- if (!historyEntryExists) createIfNotEmpty(linkColumn) else Future.successful(())
      } yield ()
    }))
  }

  private def createSimpleInit(table: Table, rowId: RowId, simples: Seq[SimpleValueColumn[_]]): Future[Seq[Unit]] = {

    def createIfNotEmpty(column: SimpleValueColumn[_]): Future[Unit] = {
      for {
        value <- retrieveCellValue(table, column, rowId)
        _ <- value match {
          case Some(v) => createSimple(table, rowId, Seq((column, Option(v))))
          case None => Future.successful(())
        }
      } yield ()
    }

    Future.sequence(simples.map({ column =>
      for {
        historyEntryExists <- historyExists(table.id, column.id, rowId, None)
        _ <- if (!historyEntryExists) createIfNotEmpty(column) else Future.successful(())
      } yield ()
    }))
  }

  private def createTranslationInit(
      table: Table,
      rowId: RowId,
      langtagColumns: Seq[(Seq[String], SimpleValueColumn[_])]
  ): Future[Seq[Unit]] = {

    val entries = for {
      (langtags, column) <- langtagColumns
      langtag <- langtags
    } yield (langtag, column)

    val columnsForLang = entries
      .groupBy({ case (langtag, _) => langtag })
      .mapValues(_.map({ case (_, columns) => columns }))
      .toSeq

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
      columnsForLang
        .flatMap({
          case (langtag, columns) =>
            columns
              .map({ column =>
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
      langtagOpt: Option[String]
  ): Future[Boolean] = {

    val whereLanguage =
      langtagOpt.map(langtag => s" AND (value -> 'value' -> '$langtag')::json IS NOT NULL").getOrElse("")

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
      cell <- tableauxModel.retrieveCell(table, column.id, rowId, isInternalCall = true)
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

  /**
    * Writes empty back link history for the linked rows that are going to be deleted.
    * The difference of the existing linked foreign row IDs and the linked foreign row IDs after the change
    * are going to be cleared.
    *
    * @param table table in which links are changed
    * @param rowId rows in which links changed
    * @param values Seq of columns an the new linked foreign row IDs
    */
  def clearBackLinksWhichWillBeDeleted(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[_] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      // we only care about links, other types are handled directly by `createCells`
      case Success((_, _, links, _)) =>
        if (links.isEmpty) {
          Future.successful(())
        } else {
          val futureSeq = links.map({
            case (linkColumn, newForeignIds) =>
              for {
                foreignIdsBeforeClearing <- tableauxModel.updateRowModel.retrieveLinkedRows(table, rowId, linkColumn)
                _ <- createClearBackLinks(table, foreignIdsBeforeClearing.diff(newForeignIds))
              } yield ()
          })
          Future.sequence(futureSeq)
        }
    }
  }

  def wrapAnnotationValue(value: String, uuid: UUID, langtagOpt: Option[String]): JsonObject = {
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
    val annotationsToBeRemoved = langtagOpt.map(Seq(_)).getOrElse(annotation.langtags)

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
            Some(wrapAnnotationValue(value, uuid, None).toString)
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
            Some(wrapAnnotationValue(value, uuid, None).toString)
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
              Some(wrapAnnotationValue(value, uuid, Some(langtag)).toString)
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
      langtag <- table.langtags.getOrElse(Seq.empty[String])
    } yield (column, Map(langtag -> None))

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simplesWithEmptyValues)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multisWithEmptyValues)
      _ <- if (links.isEmpty) Future.successful(()) else createClearLink(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else clearAttachments(table, rowId, attachments)
    } yield ()
  }

  private def clearAttachments(table: Table, rowId: RowId, columns: Seq[AttachmentColumn]): Future[_] = {
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

  def createCells(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)]): Future[Unit] = {
    val columns = values.map({ case (col: ColumnType[_], _) => col })
    val (_, _, _, attachments) = ColumnType.splitIntoTypes(columns)

    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, _)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, links, allowRecursion = true)
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

  def createClearLink(table: Table, rowId: RowId, columns: Seq[LinkColumn]): Future[Unit] = {
    for {
      _ <- createLinks(table, rowId, columns.map(column => (column, Seq.empty[RowId])), allowRecursion = false)
      _ <- Future.sequence(
        columns.map({ column =>
          {
            for {
              foreignIds <- tableauxModel.updateRowModel.retrieveLinkedRows(table, rowId, column)
              _ <- createClearBackLinks(table, foreignIds)
            } yield ()
          }
        })
      )
    } yield ()
  }

  /**
    * Writes empty back link histories for all dependent columns that have backlinks to the original table
    *
    * @param table
    * @param foreignIds
    */
  def createClearBackLinks(table: Table, foreignIds: Seq[RowId]): Future[Unit] = {
    for {
      dependentColumns <- tableauxModel.retrieveDependencies(table)
      _ <- Future.sequence(dependentColumns.map({
        case DependentColumnInformation(linkedTableId, linkedColumnId, _, _, _) =>
          for {
            linkedTable <- tableModel.retrieve(linkedTableId, isInternalCall = true)
            linkedColumn <- tableauxModel.retrieveColumn(linkedTable, linkedColumnId)
            _ <- Future.sequence(foreignIds.map(linkId => {
              for {
                _ <- insertCellHistory(linkedTable,
                                       linkId,
                                       linkedColumn.id,
                                       linkedColumn.kind,
                                       linkedColumn.languageType,
                                       wrapLinkValue())
              } yield ()
            }))
          } yield ()
      }))
    } yield ()
  }

  def deleteLink(table: Table, linkColumn: LinkColumn, rowId: RowId, toId: RowId): Future[_] = {
    if (linkColumn.linkDirection.constraint.deleteCascade) {
      createLinks(table, rowId, Seq((linkColumn, Seq(toId))), allowRecursion = false)
    } else {
      createLinks(table, rowId, Seq((linkColumn, Seq(toId))), allowRecursion = true)
    }
  }
}
