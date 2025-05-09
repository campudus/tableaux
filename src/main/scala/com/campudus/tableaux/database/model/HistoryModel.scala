package com.campudus.tableaux.database.model

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, RowPermissionSeq, TableId}
import com.campudus.tableaux.database.model.structure.TableModel
import com.campudus.tableaux.helper.{IdentifierFlattener, JsonUtils}
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

import java.util.UUID

case class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieveCell(
      table: Table,
      column: ColumnType[_],
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(Some(column.id), Some(rowId), langtagOpt, typeOpt, includeDeleted)
    retrieve(table, where, binds)
  }

  def retrieveColumn(
      table: Table,
      column: ColumnType[_],
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(Some(column.id), None, langtagOpt, typeOpt, includeDeleted, true)
    retrieve(table, where, binds)
  }

  def retrieveRow(
      table: Table,
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(None, Some(rowId), langtagOpt, typeOpt, includeDeleted)
    retrieve(table, where, binds)
  }

  def retrieveTable(
      table: Table,
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  ): Future[Seq[History]] = {
    val (where, binds) = generateWhereAndBinds(None, None, langtagOpt, typeOpt, includeDeleted)
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
      JsonUtils.parseJson(row.getString(9)),
      convertStringToDateTime(row.getString(10))
    )
  }

  private def retrieve(table: Table, where: String, binds: Seq[Any]): Future[Seq[History]] = {
    val select =
      s"""
         |SELECT
         |  revision,
         |  row_id,
         |  column_id,
         |  event,
         |  history_type,
         |  value_type,
         |  language_type,
         |  author,
         |  timestamp,
         |  value,
         |  deleted_at
         |FROM
         |  user_table_history_${table.id}
         |WHERE
         |  1 = 1
         |  $where
         |ORDER BY
         |  revision ASC
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
      typeOpt: Option[String],
      includeDeleted: Boolean,
      isStrictColumnId: Boolean = false
  ): (String, Seq[Any]) = {
    val whereColumnId: Option[(String, Option[ColumnId])] = isStrictColumnId match {
      case true => columnIdOpt.map(c => (s" AND column_id = ?", Some(c)))
      case false => columnIdOpt.map(c => (s" AND (column_id = ? OR column_id IS NULL)", Some(c)))
    }
    val whereRowId: Option[(String, Option[RowId])] = rowIdOpt.map(r => (s" AND row_id = ?", Some(r)))
    val whereLangtag: Option[(String, Option[String])] = langtagOpt.map(l =>
      (
        s"""
           |AND (
           |  language_type != 'language' OR
           |  language_type IS NULL OR
           |  (language_type = 'language' AND (value -> 'value' -> ?)::json IS NOT NULL)
           |)
         """.stripMargin,
        Some(l)
      )
    )
    val whereType: Option[(String, Option[String])] = typeOpt.map(t => (s" AND history_type = ?", Some(t)))
    val whereIncludeDeleted: Option[(String, Option[_])] = includeDeleted match {
      case true => None
      case false => Some(s" AND deleted_at IS NULL", None)
    }
    val opts: List[Option[(String, Option[Any])]] =
      List(whereColumnId, whereRowId, whereLangtag, whereType, whereIncludeDeleted)

    opts.foldLeft(("", Seq.empty[Any])) { (acc, op) =>
      {
        val (foldWhere, foldBind) = acc
        op match {
          case Some((where, bind: Option[Any])) => (
              s"$foldWhere $where",
              bind match {
                case Some(b) => foldBind :+ b
                case None => foldBind
              }
            )
          case None => acc
        }
      }
    }
  }
}

case class CreateHistoryModel(tableauxModel: TableauxModel, connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {

  val tableModel = new TableModel(connection)
  val attachmentModel = AttachmentModel(connection)

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
  )(implicit user: TableauxUser): Future[Seq[Any]] = {

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
              linkIds <- tableauxModel.retrieveCurrentLinkIds(table, column, rowId)
              identifierCellSeq <- retrieveForeignIdentifierCells(column, linkIds)
              langTags <- getLangTags(table)

              (languageType, linksData) = getLinksData(identifierCellSeq, langTags)
              _ <- insertCellHistory(table, rowId, column.id, column.kind, languageType, wrapLinkValue(linksData))

              _ <-
                if (allowRecursion) {
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
    * Writes a new history entry on base of the current linked rows for a {@code table}, a {@code linkColumn} and
    * ({@code rowIds}).
    *
    * Since we want to update the links after deleting dependent rows, we need to check if the rows have not already
    * been deleted by the delete cascade function. For example this happens for back links with "delete cascade" in only
    * one direction.
    *
    * Also make sure the links have changed before and the cache is invalidated.
    *
    * @param table
    * @param linkColumn
    * @param rowIds
    */
  def updateLinks(table: Table, linkColumn: LinkColumn, rowIds: Seq[RowId])(
      implicit user: TableauxUser
  ): Future[Seq[Unit]] = {
    Future.sequence(
      rowIds.map(rowId =>
        for {
          rowExists <- tableauxModel.retrieveRowModel.rowExists(table.id, rowId)

          _ <-
            if (rowExists) {
              updateLinks(table, linkColumn, rowId, rowExists)
            } else {
              Future.successful(())
            }
        } yield ()
      )
    )
  }

  private def updateLinks(table: Table, linkColumn: LinkColumn, rowId: RowId, rowExists: Boolean)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      linkIds <- tableauxModel.retrieveCurrentLinkIds(table, linkColumn, rowId)
      identifierCellSeq <- retrieveForeignIdentifierCells(linkColumn, linkIds)
      langTags <- getLangTags(table)
      (languageType, linksData) = getLinksData(identifierCellSeq, langTags)
      _ <- insertCellHistory(table, rowId, linkColumn.id, linkColumn.kind, languageType, wrapLinkValue(linksData))
    } yield ()
  }

  def getLangTags(table: Table): Future[Seq[String]] = {
    table.langtags.map(Future.successful).getOrElse(tableModel.retrieveGlobalLangtags())
  }

  private def retrieveForeignIdentifierCells(column: LinkColumn, linkIds: Seq[RowId])(
      implicit user: TableauxUser
  ): Future[Seq[Cell[Any]]] = {
    val linkedCellSeq = linkIds.map(linkId =>
      for {
        foreignIdentifier <- tableauxModel.retrieveCell(column.to.table, column.to.id, linkId, isInternalCall = true)
      } yield foreignIdentifier
    )
    Future.sequence(linkedCellSeq)
  }

  // oldCell only makes sense if we have a values sequence with a single cell
  // in all other cases it defaults to None
  private def createTranslation(
      table: Table,
      rowId: RowId,
      values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])],
      oldCell: Option[Cell[_]] = None
  )(implicit user: TableauxUser): Future[Unit] = {

    val oldCellJson = oldCell.map(_.getJson).getOrElse(Json.emptyObj())
    val oldCellValueMap = JsonUtils.multiLangValueToMap(oldCellJson.getJsonObject("value"))

    values.foldLeft(Future.successful(())) {
      case (accFut, (column, value)) =>
        accFut.flatMap { _ =>
          val cleanMap = JsonUtils.omitNonChanges(value, oldCellValueMap.toMap)

          if (cleanMap.isEmpty) {
            return Future.successful(())
          }

          val valueJson: JsonObject = cleanMap.foldLeft(Json.emptyObj()) {
            case (obj, (langtag, value)) =>
              obj.mergeIn(Json.obj(langtag -> value.getOrElse(null)))
          }

          insertCellHistory(
            table,
            rowId,
            column.id,
            column.kind,
            column.languageType,
            Json.obj("value" -> valueJson)
          ).map(_ => ())
        }
    }
  }

  private def createSimple(
      table: Table,
      rowId: RowId,
      simples: Seq[(SimpleValueColumn[_], Option[Any])]
  )(implicit user: TableauxUser): Future[Seq[RowId]] = {

    def wrapValue(value: Any): JsonObject = Json.obj("value" -> value)

    Future.sequence(
      simples
        .map({
          case (column: SimpleValueColumn[_], valueOpt) =>
            insertCellHistory(table, rowId, column.id, column.kind, column.languageType, wrapValue(valueOpt.orNull))
        })
    )
  }

  private def createAttachments(
      table: Table,
      rowId: RowId,
      columns: Seq[AttachmentColumn]
  )(implicit user: TableauxUser): Future[_] = {

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
  )(implicit user: TableauxUser): Future[RowId] = {
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
          user.name
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
  )(implicit user: TableauxUser): Future[RowId] = {
    logger.info(s"createCellHistory ${table.id} $columnId $rowId $json ${user.name}")
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

  private def insertCreateRowHistory(
      table: Table,
      rowId: RowId
  )(implicit user: TableauxUser): Future[RowId] = {
    logger.info(s"insertCreateRowHistory ${table.id} $rowId ${user.name}")
    insertHistory(table.id, rowId, None, RowCreatedEvent, HistoryTypeRow, None, None, None)
  }

  private def insertChangedRowPermissionHistory(
      table: Table,
      rowId: RowId,
      rowPermissionsOpt: Option[RowPermissionSeq]
  )(implicit user: TableauxUser): Future[RowId] = {
    logger.info(s"insertChangedRowPermissionHistory ${table.id} $rowId ${user.name}")
    val value = rowPermissionsOpt match {
      case Some(perm) => Json.arr(perm: _*)
      case None => null
    }
    val jsonString = Json.obj("value" -> value)
    insertHistory(
      table.id,
      rowId,
      None,
      RowPermissionsChangedEvent,
      HistoryTypeRowPermissions,
      Some("permissions"),
      None,
      Some(jsonString.toString)
    )
  }

  private def insertDeleteRowHistory(
      table: Table,
      rowId: RowId,
      replacingRowIdOpt: Option[Int]
  )(implicit user: TableauxUser): Future[RowId] = {
    logger.info(s"insertDeleteRowHistory ${table.id} $rowId ${user.name} ${replacingRowIdOpt}")
    val jsonStringOpt = replacingRowIdOpt match {
      case Some(replacingRowId) => Some(Json.obj("value" -> Json.obj("replacingRowId" -> replacingRowId)).toString())
      case None => None
    }
    insertHistory(table.id, rowId, None, RowDeletedEvent, HistoryTypeRow, None, None, jsonStringOpt)
  }

  private def createRowAnnotationHistory(
      tableId: TableId,
      rowId: RowId,
      eventType: HistoryEventType,
      rowAnnotationType: RowAnnotationType
  )(implicit user: TableauxUser): Future[RowId] = {
    val value = rowAnnotationType.toString
    logger.info(s"createRowAnnotationHistory $tableId $rowId ${eventType.toString} $value ${user.name}")
    val json = Json.obj("value" -> value)
    insertHistory(
      tableId,
      rowId,
      None,
      eventType,
      HistoryTypeRowFlag,
      Some(value),
      Some(LanguageType.NEUTRAL),
      Some(json.toString)
    )
  }

  /**
    * Creates an initial history cell value when changing a cell value (for backward compatibility).
    *
    * If we didn't call this method on any cell change/deletion the previously valid value wouldn't be logged in a
    * history table.
    */
  def createCellsInit(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)])(
      implicit user: TableauxUser
  ): Future[Unit] = {
    val columns = values.map({ case (col: ColumnType[_], _) => col })
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simples)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, multis)
      _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else createAttachmentsInit(table, rowId, attachments)
    } yield ()
  }

  /**
    * Creates an initial history cell value when deleting a cell value (for backward compatibility).
    *
    * If we didn't call this method on any cell change/deletion the previously valid value wouldn't be logged in a
    * history table.
    */
  def createClearCellInit(table: Table, rowId: RowId, columns: Seq[ColumnType[_]])(
      implicit user: TableauxUser
  ): Future[Unit] = {
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    // for clearing of multi language cells create init values for all langtags
    val langtagColumns2 = for {
      column <- multis
      langtag = table.langtags.getOrElse(Seq.empty[String])
    } yield (langtag, column)

    val langtagColumns = for {
      column <- multis
      langtag = table.langtags.getOrElse(Seq.empty[String])
      // convert Seq[String] to Map[String, null]
      langtagMap = langtag.map(l => l -> null).toMap
    } yield (column, langtagMap)

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimpleInit(table, rowId, simples)
      _ <- if (multis.isEmpty) Future.successful(()) else createTranslationInit(table, rowId, multis)
      _ <- if (links.isEmpty) Future.successful(()) else createLinksInit(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else createAttachmentsInit(table, rowId, attachments)
    } yield ()
  }

  def createAttachmentsInit(table: Table, rowId: RowId, columns: Seq[AttachmentColumn])(
      implicit user: TableauxUser
  ): Future[Seq[Unit]] = {
    Future.sequence(columns.map({ column =>
      for {
        historyEntryExists <- historyExists(table.id, column.id, rowId, None)
        _ <-
          if (!historyEntryExists) {
            for {
              currentAttachments <- attachmentModel.retrieveAll(table.id, column.id, rowId)
              _ <-
                if (currentAttachments.nonEmpty) {
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

  def createLinksInit(table: Table, rowId: RowId, links: Seq[LinkColumn])(
      implicit user: TableauxUser
  ): Future[Seq[Unit]] = {

    def createIfNotEmpty(linkColumn: LinkColumn): Future[Unit] = {
      for {
        linkIds <- tableauxModel.retrieveCurrentLinkIds(table, linkColumn, rowId)
        _ <-
          if (linkIds.nonEmpty) {
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

  private def createSimpleInit(table: Table, rowId: RowId, simples: Seq[SimpleValueColumn[_]])(
      implicit user: TableauxUser
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

    Future.sequence(simples.map({ column =>
      for {
        historyEntryExists <- historyExists(table.id, column.id, rowId, None)
        _ <- if (!historyEntryExists) createIfNotEmpty(column) else Future.successful(())
      } yield ()
    }))
  }

  private def createTranslationInit(table: Table, rowId: RowId, langtagColumns: List[SimpleValueColumn[_]])(implicit
  user: TableauxUser): Future[Seq[Unit]] = {

    def createIfNotEmpty(column: SimpleValueColumn[_]): Future[Unit] = {
      for {
        value <- retrieveCellValue(table, column, rowId)
        _ <- value match {
          case Some(v: Map[_, _]) =>
            createTranslation(table, rowId, Seq((column, v.asInstanceOf[Map[String, Option[_]]])))
          case Some(v) => {
            logger.warn(
              s"createTranslationInit: value is not a Map, but $v for column ${column.id} in table ${table.id}"
            )
            Future.successful(())
          }
          case None => Future.successful(())
        }
      } yield ()

    }
    Future.sequence(
      langtagColumns
        .map({ column =>
          for {
            historyEntryExists <- historyExists(table.id, column.id, rowId, None)
            _ <- if (!historyEntryExists) createIfNotEmpty(column) else Future.successful(())
          } yield ()
        })
    )
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
      Json.arr(columnId, rowId)
    )
  }

  private def retrieveCellValue(table: Table, column: ColumnType[_], rowId: RowId)(implicit
  user: TableauxUser): Future[Option[Any]] = {
    for {
      cell <- tableauxModel.retrieveCell(table, column.id, rowId, isInternalCall = true)
    } yield {
      Option(cell.value) match {
        case Some(v) =>
          column match {
            case MultiLanguageColumn(_) => {
              val rawValue = cell.getJson.getJsonObject("value")
              column match {
                case _ => {
                  if (rawValue.isEmpty()) {
                    None
                  } else {
                    Option(JsonUtils.multiLangValueToMap(rawValue))
                  }
                }
              }
            }
            case _: SimpleValueColumn[_] => Some(v)
          }
        case _ => None
      }
    }
  }

  /**
    * Writes empty back link history for the linked rows that are going to be deleted. The difference of the existing
    * linked foreign row IDs and the linked foreign row IDs after the change are going to be cleared.
    *
    * @param table
    *   table in which links are changed
    * @param rowId
    *   rows in which links changed
    * @param values
    *   Seq of columns an the new linked foreign row IDs
    */
  def clearBackLinksWhichWillBeDeleted(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)])(
      implicit user: TableauxUser
  ): Future[_] = {
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
  )(implicit user: TableauxUser): Future[Seq[RowId]] = {
    val languageType = if (annotation.langtags.isEmpty) LanguageNeutral else MultiLanguage
    val annotationsToBeRemoved = langtagOpt.map(Seq(_)).getOrElse(annotation.langtags)

    logger.info(
      s"createRemoveAnnotationHistory ${column.table.id} $rowId ${annotation.annotationType} ${annotation.value} ${user.name}"
    )

    insertAnnotationHistory(
      column.table,
      column,
      rowId,
      uuid,
      AnnotationRemovedEvent,
      annotationsToBeRemoved,
      annotation.annotationType,
      annotation.value,
      languageType,
      user.name
    )
  }

  def addCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      uuid: UUID,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  )(implicit user: TableauxUser): Future[Seq[RowId]] = {
    val languageType = if (langtags.isEmpty) LanguageNeutral else MultiLanguage

    logger.info(s"createAddAnnotationHistory ${column.table.id} $rowId $langtags $value ${user.name}")

    insertAnnotationHistory(
      column.table,
      column,
      rowId,
      uuid,
      AnnotationAddedEvent,
      langtags,
      annotationType,
      value,
      languageType,
      user.name
    )
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
  )(implicit user: TableauxUser): Future[Seq[RowId]] = {
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
          } yield historyRowId
        )
      case (_, MultiCountry(_)) => Seq.empty[Future[RowId]]
    }

    Future.sequence(futureSequence)
  }

  def createClearCell(table: Table, rowId: RowId, columns: Seq[ColumnType[_]], oldCell: Option[Cell[_]])(
      implicit user: TableauxUser
  ): Future[Unit] = {
    val (simples, multis, links, attachments) = ColumnType.splitIntoTypes(columns)

    val simplesWithEmptyValues = simples.map(column => (column, None))

    val multisWithEmptyValues = for {
      column <- multis
      langtag <- column.languageType match {
        case MultiLanguage => table.langtags.getOrElse(Seq.empty[String])
        case MultiCountry(countryCodes) => countryCodes.codes
      }
    } yield (column, Map(langtag -> None))

    logger.info(s"createClearCell ${table.id} $rowId multisWithEmptyValues $multisWithEmptyValues")

    for {
      _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simplesWithEmptyValues)
      _ <-
        if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multisWithEmptyValues, oldCell)
      _ <- if (links.isEmpty) Future.successful(()) else createClearLink(table, rowId, links)
      _ <- if (attachments.isEmpty) Future.successful(()) else clearAttachments(table, rowId, attachments)
    } yield ()
  }

  private def clearAttachments(table: Table, rowId: RowId, columns: Seq[AttachmentColumn])(
      implicit user: TableauxUser
  ): Future[_] = {
    val futureSequence = columns.map(column =>
      insertCellHistory(table, rowId, column.id, column.kind, column.languageType, Json.obj("value" -> Json.emptyArr()))
    )

    Future.sequence(futureSequence)
  }

  def createCells(table: Table, rowId: RowId, values: Seq[(ColumnType[_], _)], oldCell: Option[Cell[_]] = None)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multis, oldCell)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, links, allowRecursion = true)
          _ <-
            if (attachments.isEmpty) Future.successful(())
            else createAttachments(table, rowId, attachments.map({ case (column, _) => column }))
        } yield ()
    }
  }

  def createRow(table: Table, rowId: RowId, rowPermissionsOpt: Option[Seq[String]])(
      implicit user: TableauxUser
  ): Future[RowId] = {
    for {
      _ <- insertCreateRowHistory(table, rowId)
      _ <- rowPermissionsOpt match {
        case Some(rowPermissions) => insertChangedRowPermissionHistory(table, rowId, rowPermissionsOpt)
        case None => Future.successful(())
      }
    } yield rowId
  }

  def updateRowPermission(table: Table, rowId: RowId, rowPermissionsOpt: Option[RowPermissionSeq])(
      implicit user: TableauxUser
  ): Future[RowId] = {
    insertChangedRowPermissionHistory(table, rowId, rowPermissionsOpt)
  }

  def deleteRow(table: Table, rowId: RowId, replacingRowIdOpt: Option[Int])(
      implicit user: TableauxUser
  ): Future[RowId] = {
    insertDeleteRowHistory(table, rowId, replacingRowIdOpt)
  }

  def createRowsAnnotationHistory(
      tableId: TableId,
      isAdd: Boolean,
      rowAnnotationType: RowAnnotationType,
      rowIds: Seq[RowId]
  )(implicit user: TableauxUser): Future[Seq[RowId]] = {
    val eventType = if (isAdd) AnnotationAddedEvent else AnnotationRemovedEvent
    Future.sequence(rowIds.map(rowId => createRowAnnotationHistory(tableId, rowId, eventType, rowAnnotationType)))
  }

  def createClearLink(table: Table, rowId: RowId, columns: Seq[LinkColumn])(
      implicit user: TableauxUser
  ): Future[Unit] = {
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
  def createClearBackLinks(table: Table, foreignIds: Seq[RowId])(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      dependentColumns <- tableauxModel.retrieveDependencies(table)
      _ <- Future.sequence(dependentColumns.map({
        case DependentColumnInformation(linkedTableId, linkedColumnId, _, _, _) =>
          for {
            linkedTable <- tableModel.retrieve(linkedTableId, isInternalCall = true)
            linkedColumn <- tableauxModel.retrieveColumn(linkedTable, linkedColumnId)
            _ <- Future.sequence(foreignIds.map(linkId => {
              for {
                _ <- insertCellHistory(
                  linkedTable,
                  linkId,
                  linkedColumn.id,
                  linkedColumn.kind,
                  linkedColumn.languageType,
                  wrapLinkValue()
                )
              } yield ()
            }))
          } yield ()
      }))
    } yield ()
  }

  // TODO delete b/c not used?
  def deleteLink(table: Table, linkColumn: LinkColumn, rowId: RowId, toId: RowId)(
      implicit user: TableauxUser
  ): Future[_] = {
    if (linkColumn.linkDirection.constraint.deleteCascade) {
      createLinks(table, rowId, Seq((linkColumn, Seq(toId))), allowRecursion = false)
    } else {
      createLinks(table, rowId, Seq((linkColumn, Seq(toId))), allowRecursion = true)
    }
  }
}
