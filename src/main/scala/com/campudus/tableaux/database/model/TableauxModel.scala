package com.campudus.tableaux.database.model

import com.campudus.tableaux._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{CreateRowModel, RetrieveRowModel, UpdateRowModel}
import com.campudus.tableaux.helper.JsonUtils.asSeqOf
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.verticles.EventClient

import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}
import scala.util.Try
import scala.util.control.NonFatal

import java.util.UUID

object TableauxModel {
  type LinkId = Long
  type TableGroupId = Long
  type TableId = Long
  type ColumnId = Long
  type RowId = Long

  type Ordering = Long

  type RowPermissionSeq = Seq[String]

  def apply(connection: DatabaseConnection, structureModel: StructureModel, config: TableauxConfig)(
      implicit roleModel: RoleModel
  ): TableauxModel = {
    new TableauxModel(connection, structureModel, config)
  }
}

class DuplicateRowOptions(
    val skipConstrainedFrom: Boolean,
    val annotateSkipped: Boolean,
    val columnIds: Option[Seq[TableauxModel.ColumnId]] = None
)

object DuplicateRowOptions {

  def apply(
      skipConstrainedFrom: Boolean,
      annotateSkipped: Boolean,
      columnIds: Option[Seq[TableauxModel.ColumnId]] = None
  ): DuplicateRowOptions = {
    new DuplicateRowOptions(skipConstrainedFrom, annotateSkipped, columnIds)
  }
}

/**
  * Needed because e.g. `TableauxController#createCompleteTable` and `TableauxController#retrieveCompleteTable` need to
  * call method from `StructureModel`.
  */
sealed trait StructureDelegateModel extends DatabaseQuery {

  import TableauxModel._

  protected val connection: DatabaseConnection

  protected val structureModel: StructureModel

  protected val config: TableauxConfig

  protected[this] implicit def roleModel: RoleModel

  def createTable(name: String, hidden: Boolean)(implicit user: TableauxUser): Future[Table] = {
    structureModel.tableStruc.create(name, hidden, None, List(), GenericTable, None, None, None, None)
  }

  def retrieveTable(tableId: TableId, isInternalCall: Boolean = false)(
      implicit user: TableauxUser
  ): Future[Table] = {
    structureModel.tableStruc.retrieve(tableId, isInternalCall)
  }

  def retrieveTables(isInternalCall: Boolean = false)(implicit user: TableauxUser): Future[Seq[Table]] = {
    structureModel.tableStruc.retrieveAll(isInternalCall)
  }

  def createColumns(table: Table, columns: Seq[CreateColumn])(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[_]]] = {
    structureModel.columnStruc.createColumns(table, columns)
  }

  def retrieveColumn(table: Table, columnId: ColumnId)(
      implicit user: TableauxUser
  ): Future[ColumnType[_]] = {
    structureModel.columnStruc.retrieve(table, columnId)
  }

  def retrieveColumns(table: Table, isInternalCall: Boolean = false)(
      implicit user: TableauxUser
  ): Future[Seq[ColumnType[_]]] = {
    for {
      allColumns <- structureModel.columnStruc.retrieveAll(table)
      filteredColumns = roleModel
        .filterDomainObjects[ColumnType[_]](ViewColumn, allColumns, ComparisonObjects(table), isInternalCall)

    } yield filteredColumns
  }

  def retrieveDependencies(table: Table): Future[Seq[DependentColumnInformation]] = {
    structureModel.columnStruc.retrieveDependencies(table.id)
  }

  def retrieveDependentGroupColumns(column: ColumnType[_]): Future[Seq[DependentColumnInformation]] = {
    structureModel.columnStruc.retrieveDependentGroupColumn(column.table.id, column.id)
  }

  def retrieveDependentLinks(table: Table): Future[Seq[(LinkId, LinkDirection)]] = {
    structureModel.columnStruc.retrieveDependentLinks(table.id)
  }
}

class TableauxModel(
    override protected[this] val connection: DatabaseConnection,
    override protected[this] val structureModel: StructureModel,
    override protected[this] val config: TableauxConfig
)(override implicit val roleModel: RoleModel)
    extends DatabaseQuery
    with StructureDelegateModel {

  import TableauxModel._

  val retrieveRowModel = new RetrieveRowModel(connection)
  val createRowModel = new CreateRowModel(connection)
  val updateRowModel = new UpdateRowModel(connection)

  val attachmentModel = AttachmentModel(connection)
  val retrieveHistoryModel = RetrieveHistoryModel(connection)
  val createHistoryModel = CreateHistoryModel(this, connection)
  val eventClient = EventClient(connection.vertx)

  def retrieveBacklink(column: LinkColumn)(implicit user: TableauxUser): Future[Option[LinkColumn]] = {
    val select =
      s"""
         |SELECT
         |  columns_back.column_id
         |FROM
         |  system_columns c
         |JOIN system_link_table l ON (l.link_id = c.link_id)
         |JOIN system_columns columns_back ON (c.link_id = columns_back.link_id AND columns_back.table_id != ?)
         |WHERE c.table_id = ? AND c.column_id = ?""".stripMargin

    for {
      foreignColumnIdOpt <- connection
        .query(select, Json.arr(column.table.id, column.table.id, column.id))
        .map(json => resultObjectToJsonArray(json).headOption.map(row => row.get[ColumnId](0)))

      backlinkColumnOpt <- foreignColumnIdOpt match {
        case Some(columnId) =>
          structureModel.columnStruc.retrieve(column.to.table, columnId).map(res => Some(res.asInstanceOf[LinkColumn]))
        case None => Future.successful(None)
      }
    } yield backlinkColumnOpt
  }

  private def selectDependentRows(linkId: LinkId, linkDirection: LinkDirection) = {
    if (linkDirection.from == linkDirection.to) {
      s"SELECT ${linkDirection.fromSql} FROM link_table_$linkId WHERE ${linkDirection.toSql} = ?"
    } else {
      s"SELECT ${linkDirection.toSql} FROM link_table_$linkId WHERE ${linkDirection.fromSql} = ?"
    }
  }

  private def retrieveDependentTableAndRowIds(
      linkId: LinkId,
      linkDirection: LinkDirection,
      rowId: RowId
  ): Future[(TableId, Seq[ColumnId])] = {
    connection
      .query(selectDependentRows(linkId, linkDirection), Json.arr(rowId))
      .map(result => resultObjectToJsonArray(result).map(_.getLong(0).longValue()))
      .map(dependentRows => (linkDirection.to, dependentRows))
  }

  def retrieveDependentRows(table: Table, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[DependentRowsSeq] = {

    for {
      links <- retrieveDependentLinks(table)
      result <- {
        val futures = links.map({
          case (linkId, linkDirection) =>
            retrieveDependentTableAndRowIds(linkId, linkDirection, rowId)
              .flatMap({
                case (tableId, rows) =>
                  for {
                    table <- retrieveTable(tableId, isInternalCall = true)
                    columns <- retrieveColumns(table, isInternalCall = true)
                    rowObjects <- Future.sequence(rows.map({ rowId =>
                      {
                        retrieveCell(columns.head, rowId, true)
                          .map({ cell =>
                            {
                              Json.obj("id" -> rowId, "value" -> cell.value)
                            }
                          })
                      }
                    }))
                  } yield (table, columns.head, rowObjects)
              })
        })

        Future.sequence(futures)
      }
    } yield {
      val objects = result
        .groupBy({ case (dependentTable, column, _) => (dependentTable, column) })
        .map({
          case ((groupedByTable, groupedByColumn), dependentRowInformation) =>
            (
              groupedByTable,
              groupedByColumn,
              dependentRowInformation
                .flatMap({
                  case (_, _, values) => values
                })
                .distinct
            )
        })
        .filter({
          case (_, _, values) => values.nonEmpty
        })
        .map({
          case (groupedByTable, groupedByColumn, dependentRows) =>
            DependentRows(groupedByTable, groupedByColumn, dependentRows)
        })
        .toSeq

      DependentRowsSeq(objects)
    }
  }

  /**
    * Retrieves all dependent cells information for a given {@code table} and {@code rowId}.
    *
    * @param table
    * @param rowId
    * @return
    */
  def retrieveDependentCells(table: Table, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Seq[(Table, LinkColumn, Seq[RowId])]] = {
    for {
      links <- retrieveDependentLinks(table)
      result <- Future.sequence(
        links.map {
          case (linkId, linkDirection) =>
            retrieveDependentTableAndRowIds(linkId, linkDirection, rowId).flatMap {
              case (dependentTableId, dependentRows) =>
                retrieveTable(dependentTableId).flatMap { dependentTable =>
                  retrieveColumns(dependentTable).map(_.collectFirst {
                    case c: LinkColumn if c.linkId == linkId => (dependentTable, c, dependentRows)
                  })
                }
            }
        }
      )
    } yield result.flatten
  }

  def deleteRow(table: Table, rowId: RowId, replacingRowIdOpt: Option[Int] = None)(
      implicit user: TableauxUser
  ): Future[EmptyObject] = {
    doDeleteRow(table, rowId, replacingRowIdOpt)
  }

  def deleteRow(table: Table, rowId: RowId)(implicit user: TableauxUser): Future[EmptyObject] = {
    doDeleteRow(table, rowId, None)
  }

  def retrieveCurrentLinkIds(table: Table, column: LinkColumn, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Seq[RowId]] = {
    for {
      cell <- retrieveCell(table, column.id, rowId, isInternalCall = true)
    } yield {
      cell.getJson
        .getJsonArray("value")
        .asScala
        .map(_.asInstanceOf[JsonObject])
        .map(_.getLong("id").longValue())
        .toSeq
    }
  }

  def doDeleteRow(
      table: Table,
      rowId: RowId,
      replacingRowIdOpt: Option[Int] = None
  )(implicit user: TableauxUser): Future[EmptyObject] = {

    val fnc = (t: Option[DbTransaction]) => {

      for {
        columns <- retrieveColumns(table, isInternalCall = true)

        specialColumns = columns.filter({
          case _: AttachmentColumn => true
          case c: LinkColumn if c.linkDirection.constraint.deleteCascade => true
          case _ => false
        })

        // enrich with dummy value, is necessary for validity checks but thrown away afterward
        columnValueLinks = columns
          .collect({ case c: LinkColumn if !c.linkDirection.constraint.deleteCascade => c })
          .map(col => (col, 0))

        _ <- createHistoryModel.createCellsInit(table, rowId, columnValueLinks)

        linkList <- retrieveDependentCells(table, rowId)

        // get already replaced ids before we delete the row
        replacedRowIds <- updateRowModel.getReplacedIds(table.id, rowId, t)

        // Clear special cells before delete.
        // For example AttachmentColumns will
        // not be deleted by DELETE CASCADE.
        // clearing LinkColumn will eventually trigger delete cascade
        _ <- updateRowModel.clearRow(table, rowId, specialColumns, deleteRow, t)
        _ <- updateRowModel.deleteRow(table.id, rowId, t)

        // first delete row and then update dependent rows because of constraint checks
        // we can do that because of the transaction
        _ <- replacingRowIdOpt match {
          case Some(replacingRowId) => {
            for {
              _ <- updateRowModel.updateReplacedIds(table.id, replacedRowIds, rowId, replacingRowId, t)

              // link dependent Rows to new row
              // use foldLeft and flatMap to execute queries sequentially, as a transaction doesn't allow
              // multiple queries at the same time
              _ <- linkList.flatMap({
                case (table, column, rowIds) => {
                  rowIds.map(id => (table, column, id))
                }
              }).foldLeft(Future(())) {
                {
                  case (future, (table, column, id)) => {
                    for {
                      _ <- future
                      linkedIds <- retrieveCurrentLinkIds(table, column, id)

                      _ <-
                        if (!linkedIds.contains(replacingRowId)) {
                          updateOrReplaceValue(table, column.id, id, replacingRowId, false, maybeTransaction = t)
                        } else {
                          logger.info(
                            s"skip adding link ${replacingRowId} to: ${table.id} ${column.id} $id because it is already linked"
                          )
                          Future.successful(())
                        }
                    } yield ()
                  }
                }
              }
            } yield ()
          }
          case None => Future.successful(())
        }

        // invalidate row
        _ <- eventClient.invalidateRow(table.id, rowId)

        _ <- Future.sequence(
          linkList.map({
            case (dependentTable, dependentLinkColumn, dependentRows) =>
              for {
                _ <- invalidateCellAndDependentColumns(dependentLinkColumn, dependentRows)
                _ <- createHistoryModel.updateLinks(dependentTable, dependentLinkColumn, dependentRows)
              } yield ()
          })
        )

      } yield (t, EmptyObject())
    };

    for {
      _ <- replacingRowIdOpt match {
        case Some(_) => connection.transactional(t => fnc(Some(t)).map({ case (t, obj) => (t.get, obj) }))
        case None => fnc(None).map({ case (_, emptyObj) => emptyObj })
      }

      _ <- createHistoryModel.deleteRow(table, rowId, replacingRowIdOpt)

    } yield EmptyObject()

  }

  def createRow(table: Table, rowPermissionsOpt: Option[Seq[String]])(implicit user: TableauxUser): Future[Row] = {
    for {
      rowId <- createRowModel.createRow(table, Seq.empty, rowPermissionsOpt)
      _ <- createHistoryModel.createRow(table, rowId, rowPermissionsOpt)
      row <- retrieveRow(table, rowId)
    } yield row
  }

  def createRows(table: Table, rows: Seq[Seq[(ColumnId, Any)]], rowPermissionsOpt: Option[Seq[String]])(
      implicit user: TableauxUser
  ): Future[RowSeq] = {
    for {
      allColumns <- retrieveColumns(table)
      columns = roleModel.filterDomainObjects(ViewColumn, allColumns, ComparisonObjects(table), isInternalCall = false)
      rows <- rows.foldLeft(Future.successful(Vector[Row]())) { (futureRows, row) => // replace ColumnId with ColumnType
        // TODO fail nice if columnid doesn't exist
        {
          val columnValuePairs = row.map { case (columnId, value) => (columns.find(_.id == columnId).get, value) }

          futureRows.flatMap { rows =>
            for {

              // TODO add more checks like unique etc. for all tables depending on column configuration
              _ <- table.tableType match {
                case SettingsTable => {

                  val keyColumn = columns.find(_.id == 1).orNull
                  val keyName = row
                    .find({ case (id, _) => id == 1 })
                    .flatMap({ case (_, colName) => Option(colName) })

                  for {
                    _ <- checkForEmptyKey(table, keyName)
                    _ <- checkForDuplicateKey(table, keyColumn, keyName)
                  } yield ()
                }
                case _ => Future.successful(())
              }

              rowId <- createRowModel.createRow(table, columnValuePairs, rowPermissionsOpt)
              _ <- createHistoryModel.createRow(table, rowId, rowPermissionsOpt)
              _ <- createHistoryModel.createCells(table, rowId, columnValuePairs)

              newRow <- retrieveRow(table, columns, rowId)
            } yield {
              rows ++ Seq(newRow)
            }
          }
        }
      }
    } yield RowSeq(rows)
  }

  def updateRow(
      table: Table,
      rowId: RowId,
      rowUpdate: Seq[(ColumnId, Any)],
      rowPermissionOpt: Option[Seq[String]],
      forceHistory: Boolean = false
  )(implicit user: TableauxUser): Future[Seq[Cell[_]]] = {
    for {
      allColumns <- retrieveColumns(table)
      columns = roleModel.filterDomainObjects(ViewColumn, allColumns, ComparisonObjects(table), isInternalCall = false)
      updateEffects = rowUpdate.flatMap {
        case (columnId, value) =>
          columns.find(_.id == columnId) map {
            column => updateCellValue(table, column.id, rowId, value, forceHistory)
          }
      }
      futureOfSeq <- Future.sequence(updateEffects)
    } yield futureOfSeq
  }

  def setRow(
      table: Table,
      rowId: RowId,
      rowUpdate: Seq[(ColumnId, Any)],
      rowPermissionOpt: Option[Seq[String]],
      forceHistory: Boolean = false
  )(implicit user: TableauxUser): Future[Seq[Cell[_]]] = {
    for {
      allColumns <- retrieveColumns(table)
      columns = roleModel.filterDomainObjects(ViewColumn, allColumns, ComparisonObjects(table), isInternalCall = false)
      updateEffects = rowUpdate.flatMap {
        case (columnId, value) =>
          columns.find(_.id == columnId) map {
            column => replaceCellValue(table, column.id, rowId, value, forceHistory)
          }
      }
      futureOfSeq <- Future.sequence(updateEffects)
    } yield futureOfSeq
  }

  def addCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  )(implicit user: TableauxUser): Future[CellLevelAnnotation] = {
    for {
      (uuid, mergedLangtags, createdAt) <- updateRowModel.addOrMergeCellAnnotation(
        column,
        rowId,
        langtags,
        annotationType,
        value
      )
      _ <- createHistoryModel.addCellAnnotation(column, rowId, uuid, langtags, annotationType, value)
    } yield CellLevelAnnotation(uuid, annotationType, mergedLangtags, value, createdAt)
  }

  def deleteCellAnnotation(column: ColumnType[_], rowId: RowId, uuid: UUID)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      annotationOpt <- retrieveRowModel.retrieveAnnotation(column.table.id, rowId, column, uuid)
      _ <- annotationOpt match {
        case Some(annotation) => createHistoryModel.removeCellAnnotation(column, rowId, uuid, annotation)
        case None => Future.successful(())
      }
      _ <- updateRowModel.deleteCellAnnotation(column, rowId, uuid)
    } yield ()
  }

  def deleteCellAnnotation(column: ColumnType[_], rowId: RowId, uuid: UUID, langtag: String)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      annotationOpt <- retrieveRowModel.retrieveAnnotation(column.table.id, rowId, column, uuid)
      _ <- annotationOpt match {
        case Some(annotation) => createHistoryModel.removeCellAnnotation(column, rowId, uuid, annotation, Some(langtag))
        case None => Future.successful(())
      }
      _ <- updateRowModel.deleteCellAnnotation(column, rowId, uuid, langtag)
    } yield ()
  }

  def updateRowAnnotationsRecursive(
      table: Table,
      rowId: RowId,
      rowAnnotationType: RowAnnotationType,
      flag: Boolean
  )(implicit user: TableauxUser): Future[_] = {
    for {
      _ <- updateRowModel.updateRowAnnotation(table.id, rowId, rowAnnotationType, flag)
      _ <- createHistoryModel.createRowsAnnotationHistory(table.id, flag, rowAnnotationType, Seq(rowId))
      columns <- retrieveColumns(table, isInternalCall = true)

      // filter cascade columns and call updateRowAnnotationsRecursive recursively
      specialColumns = rowAnnotationType match {
        case RowAnnotationTypeFinal => columns.collect({
            case c: LinkColumn if c.linkDirection.constraint.finalCascade => c
          })
        case RowAnnotationTypeArchived => columns.collect({
            case c: LinkColumn if c.linkDirection.constraint.archiveCascade => c
          })
      }

      _ = specialColumns.map((column) => {
        for {
          _ <-
            updateRowModel.updateCascadedRows(
              table,
              rowId,
              column,
              rowAnnotationType,
              flag,
              updateRowAnnotationsRecursive
            )
        } yield ()
      })

      _ <- eventClient.invalidateRowLevelAnnotations(table.id, rowId)
    } yield ()

  };

  def updateRowAnnotations(table: Table, rowId: RowId, finalFlagOpt: Option[Boolean], archivedFlagOpt: Option[Boolean])(
      implicit user: TableauxUser
  ): Future[Row] = {

    for {
      _ <- finalFlagOpt match {
        case None => Future.successful(())
        case Some(isFinal) => updateRowAnnotationsRecursive(table, rowId, RowAnnotationTypeFinal, isFinal)
      }

      _ <- archivedFlagOpt match {
        case None => Future.successful(())
        case Some(isArchived) => updateRowAnnotationsRecursive(table, rowId, RowAnnotationTypeArchived, isArchived)
      }

      row <- retrieveRow(table, rowId)
    } yield row
  }

  def updateRowsAnnotations(table: Table, finalFlagOpt: Option[Boolean], archivedFlagOpt: Option[Boolean])(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      columns <- retrieveColumns(table, isInternalCall = true)
      rowSeq <- retrieveRowModel.retrieveAll(table.id, columns, None, None, Pagination(None, None))
      _ <- Future.sequence(
        rowSeq.map(row => updateRowAnnotations(table, row.id, finalFlagOpt, archivedFlagOpt))
      )
      _ <- eventClient.invalidateTableRowLevelAnnotations(table.id)
    } yield ()
  }

  def retrieveTableWithCellAnnotations(table: Table)(implicit user: TableauxUser): Future[TableWithCellAnnotations] = {
    retrieveTablesWithCellAnnotations(Seq(table)).map({ annotations =>
      annotations.headOption.getOrElse(TableWithCellAnnotations(table, Map.empty))
    })
  }

  def retrieveTablesWithCellAnnotations(tables: Seq[Table])(
      implicit user: TableauxUser
  ): Future[Seq[TableWithCellAnnotations]] = {
    retrieveRowModel.retrieveTablesWithCellAnnotations(tables)
  }

  def retrieveTablesWithCellAnnotationCount(tables: Seq[Table]): Future[Seq[TableWithCellAnnotationCount]] = {
    val tableIds = tables.map({ case Table(id, _, _, _, _, _, _, _, _, _) => id })

    for {
      annotationCountMap <- retrieveRowModel.retrieveCellAnnotationCount(tableIds)
      totalSizeMap <- Future.sequence(tables.map(table => retrieveTotalSize(table).map((table, _))))
    } yield {
      totalSizeMap.map({
        case (table, count) =>
          val annotationCount = annotationCountMap.getOrElse(table.id, Seq.empty)
          TableWithCellAnnotationCount(table, count, annotationCount)
      })
    }
  }

  def deleteLink(table: Table, columnId: ColumnId, rowId: RowId, toId: RowId)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column))

      _ <- column match {
        case linkColumn: LinkColumn => {
          for {
            _ <- createHistoryModel.createCellsInit(table, rowId, Seq((column, Seq(toId))))
            _ <- updateRowModel.deleteLink(table, linkColumn, rowId, toId, deleteRow)
            _ <- invalidateCellAndDependentColumns(column, rowId)
            _ <- createHistoryModel.deleteLink(table, linkColumn, rowId, toId)
          } yield Future.successful(())
        }
        case _ => Future.failed(WrongColumnKindException(column, classOf[LinkColumn]))
      }

      updatedCell <- retrieveCell(column, rowId, true)
    } yield updatedCell
  }

  def updateCellLinkOrder(
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      toId: RowId,
      locationType: LocationType
  )(implicit user: TableauxUser): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column))

      _ <- column match {
        case linkColumn: LinkColumn => {
          for {
            _ <- createHistoryModel.createCellsInit(table, rowId, Seq((linkColumn, Seq(rowId))))
            _ <- updateRowModel.updateLinkOrder(table, linkColumn, rowId, toId, locationType)
            _ <- invalidateCellAndDependentColumns(column, rowId)
            _ <- createHistoryModel.updateLinks(table, linkColumn, Seq(rowId))
          } yield Future.successful(())
        }
        case _ => Future.failed(WrongColumnKindException(column, classOf[LinkColumn]))
      }

      updatedCell <- retrieveCell(column, rowId, true)
    } yield updatedCell
  }

  private def checkValueTypeForColumn[A](column: ColumnType[_], value: A): Future[Unit] = {
    (column match {
      case MultiLanguageColumn(c) => MultiLanguageColumn.checkValidValue(c, value)
      case c => c.checkValidValue(value)
    }) match {
      case Success(_) => Future.successful(())
      case Failure(ex) => Future.failed(ex)
    }
  }

  private def checkValueLengthOfTextCell[A](column: ColumnType[_], value: A): Future[Unit] = {

    val getLengthLimitAttributes: (ColumnType[_]) => (Option[Int], Option[Int]) = (column) => {
      val minLength = column.columnInformation.minLength
      val maxLength = column.columnInformation.maxLength
      (minLength, maxLength)
    }

    val checkMultiLangLength: (Int => Boolean, JsonObject) => Boolean = (compareFunc, multiLangValue) => {
      val map: mutable.Map[String, AnyRef] = multiLangValue.getMap.asScala
      map.values.map(value => value.asInstanceOf[String]).forall(value =>
        value.length() == 0 || compareFunc(value.length())
      )
    }

    val checkSimpleLangLength: (Int => Boolean, String) => Boolean =
      (compareFunc, value) => {
        value.length() == 0 || compareFunc(value.length())
      }

    val checkLength: (Int => Boolean, Boolean, A) => Boolean = (compareFunc, isMultilang, value) => {
      isMultilang match {
        case false => checkSimpleLangLength(compareFunc, value.asInstanceOf[String])
        case true => checkMultiLangLength(compareFunc, value.asInstanceOf[JsonObject])
      }
    }

    val checkValueLength: ((Option[Int], Option[Int]), A) => Future[Unit] = (maybeLengthLimits, value) => {
      val (maybeMinLength, maybeMaxLength) = maybeLengthLimits
      val columnIsMultilanguage = column.languageType == MultiLanguage
      val isValueInRange = maybeLengthLimits match {
        case (Some(minLength), Some(maxLength)) => {
          checkLength(minLength <= _, columnIsMultilanguage, value) && checkLength(
            maxLength >= _,
            columnIsMultilanguage,
            value
          )
        }
        case (None, Some(maxLength)) => {
          checkLength(maxLength >= _, columnIsMultilanguage, value)
        }
        case (Some(minLength), None) => {
          checkLength(minLength <= _, columnIsMultilanguage, value)
        }
        case (None, None) => {
          true
        }
      }
      isValueInRange match {
        case true => Future.successful(())
        case false => Future.failed(new LengthOutOfRangeException())
      }

    }
    val maybeLengthLimits = getLengthLimitAttributes(column)

    column match {
      case col: TextColumn => checkValueLength(maybeLengthLimits, value)
      case col: RichTextColumn => checkValueLength(maybeLengthLimits, value)
      case col: ShortTextColumn => checkValueLength(maybeLengthLimits, value)
      case _ => Future.successful(())
    }
  }

  private def hasCellChanged(oldCell: Cell[_], newCell: Cell[_]): Boolean = oldCell.value != newCell.value

  private def updateOrReplaceValue[A](
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      value: A,
      replace: Boolean = false,
      forceHistory: Boolean = false,
      maybeTransaction: Option[DbTransaction] = None
  )(implicit user: TableauxUser): Future[Cell[_]] = {
    for {
      _ <- checkForSettingsTable(table, columnId, "can't update key cell of a settings table")

      column <- retrieveColumn(table, columnId)
      _ <- checkValueTypeForColumn(column, value)
      _ <- checkValueLengthOfTextCell(column, value)

      _ <-
        if (replace && column.languageType == MultiLanguage) {
          val valueJson: JsonObject = value match {
            case j: JsonObject => j
            case _ => Json.emptyObj()
          }

          val enrichedValue = roleModel.generateLangtagCheckValue(table, valueJson.copy())
          roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column, enrichedValue))
        } else {
          roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column, value))
        }

      oldCell <- retrieveCell(column, rowId, true)
      _ <- createHistoryModel.createCellsInit(table, rowId, Seq((column, value)))

      _ <-
        if (replace) {
          for {
            _ <- createHistoryModel.clearBackLinksWhichWillBeDeleted(table, rowId, Seq((column, value)))
            _ <- updateRowModel.clearRowWithValues(table, rowId, Seq((column, value)), deleteRow)
          } yield ()
        } else {
          Future.successful(())
        }

      _ <- updateRowModel.updateRow(table, rowId, Seq((column, value)), maybeTransaction)
      _ <- invalidateCellAndDependentColumns(column, rowId)
      newCell <- retrieveCell(column, rowId, true)

      shouldCreateHistory =
        forceHistory || hasCellChanged(oldCell, newCell) // does not work for multi language/currency cells

      _ <-
        if (shouldCreateHistory) {
          createHistoryModel.createCells(table, rowId, Seq((column, value)), Some(oldCell))
        } else {
          Future.successful(())
        }
    } yield newCell
  }

  def updateCellValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A, forceHistory: Boolean = false)(
      implicit user: TableauxUser
  ): Future[Cell[_]] =
    updateOrReplaceValue(table, columnId, rowId, value, forceHistory = forceHistory)

  def replaceCellValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A, forceHistory: Boolean = false)(
      implicit user: TableauxUser
  ): Future[Cell[_]] =
    updateOrReplaceValue(table, columnId, rowId, value, replace = true, forceHistory = forceHistory)

  def clearCellValue(table: Table, columnId: ColumnId, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Cell[_]] = {
    for {
      _ <- checkForSettingsTable(table, columnId, "can't clear key cell of a settings table")

      column <- retrieveColumn(table, columnId)
      oldCell <- retrieveCell(column, rowId, true)
      _ <-
        if (column.languageType == MultiLanguage) {
          val enrichedValue = roleModel.generateLangtagCheckValue(table)
          roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column, enrichedValue))
        } else {
          roleModel.checkAuthorization(EditCellValue, ComparisonObjects(table, column))
        }

      _ <- createHistoryModel.createClearCellInit(table, rowId, Seq(column))
      _ <- createHistoryModel.createClearCell(table, rowId, Seq(column), Some(oldCell))
      _ <- updateRowModel.clearRow(table, rowId, Seq(column), deleteRow)
      _ <- invalidateCellAndDependentColumns(column, rowId)

      clearedCell <- retrieveCell(column, rowId, true)
    } yield clearedCell
  }

  private def checkForSettingsTable[A](table: Table, columnId: ColumnId, exceptionMessage: String) = {
    (table.tableType, columnId) match {
      case (SettingsTable, 1 | 2) =>
        Future.failed(ForbiddenException(exceptionMessage, "cell"))
      case _ => Future.successful(())
    }
  }

  private def checkForDuplicateKey[A](table: Table, keyColumn: ColumnType[_], keyName: Option[Any])(
      implicit user: TableauxUser
  ) = {
    retrieveRows(table, Seq(keyColumn), None, None, Pagination(None, None))
      .map(_.rows.map(_.values.head))
      .flatMap(oldValues => {
        if (keyName.isDefined && oldValues.contains(keyName.get)) {
          Future.failed(ShouldBeUniqueException("Key should be unique", "cell"))
        } else {
          Future.successful(())
        }
      })
  }

  private def checkForEmptyKey[A](table: Table, keyName: Option[Any]) = {
    keyName match {
      case Some(key: String) if !key.isEmpty =>
        Future.successful(())
      case _ => Future.failed(InvalidRequestException("Key must not be empty and a string in settings table"))
    }
  }

  def invalidateCellAndDependentColumns(column: ColumnType[_], rowIds: Seq[RowId])(
      implicit user: TableauxUser
  ): Future[Seq[Unit]] = {
    Future.sequence(
      rowIds.map(rowId =>
        for {
          _ <- invalidateCellAndDependentColumns(column, rowId)
        } yield ()
      )
    )
  }

  def retrieveAllStatusColumnsForTable(table: Table)(
      implicit user: TableauxUser
  ): Future[Seq[StatusColumn]] = {
    for {
      allColumns <- structureModel.columnStruc.retrieveAll(table)
    } yield allColumns.filter(column => column.kind == StatusType).map(column => column.asInstanceOf[StatusColumn])
  }

  def maybeInvalidateStatusCells(column: ColumnType[_], rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      statusColumns <- retrieveAllStatusColumnsForTable(column.table)
      _ = statusColumns.foreach((statusColumn: StatusColumn) => {
        if (statusColumn.columns.map(column => column.id).contains(column.id)) {
          eventClient.invalidateCellValue(statusColumn.table.id, statusColumn.id, rowId)
        }
      })
    } yield ()
  }

  def invalidateCellAndDependentColumns(column: ColumnType[_], rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    def invalidateColumn: (TableId, ColumnId) => Future[_] = eventClient.invalidateColumn

    for {
      // invalidate the cell itself
      _ <- eventClient.invalidateCellValue(column.table.id, column.id, rowId)

      // invalidate Status cell if it exists and has dependency on this column
      _ <- maybeInvalidateStatusCells(column, rowId)

      // invalidate the concat cell if column is an identifier
      _ <-
        if (column.identifier) {
          eventClient.invalidateCellValue(column.table.id, 0, rowId)
        } else {
          Future.successful(())
        }

      _ <-
        if (column.columnInformation.groupColumnIds.nonEmpty) {
          Future.sequence(column.columnInformation.groupColumnIds.map(invalidateColumn(column.table.id, _)))
        } else {
          Future.successful(())
        }

      dependentGroupColumns <- retrieveDependentGroupColumns(column)
      dependentLinkColumns <- retrieveDependencies(column.table)
      dependentColumns = dependentGroupColumns ++ dependentLinkColumns

      _ <- Future.sequence(dependentColumns.map({
        // We could invalidate less cache if we would know the depending rows
        // ... but this would require us to retrieve them which is definitely more expensive

        case DependentColumnInformation(tableId, columnId, _, _, groupColumnIds) =>
          // Invalidate depending link column...
          val invalidateLinkColumn = invalidateColumn(tableId, columnId)
          // Invalidate the table's concat column - to be sure...
          val invalidateConcatColumn = invalidateColumn(tableId, 0)
          // Invalidate all depending group columns
          val invalidateGroupColumns = Future.sequence(groupColumnIds.map(invalidateColumn(tableId, _)))

          invalidateLinkColumn.zip(invalidateConcatColumn).zip(invalidateGroupColumns)

      }))
    } yield ()
  }

  def retrieveCellAnnotations(
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      isInternalCall: Boolean = true
  )(implicit user: TableauxUser): Future[CellLevelAnnotations] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(ViewCellValue, ComparisonObjects(table, column), isInternalCall)
      (_, _, cellLevelAnnotations) <- retrieveRowModel.retrieveAnnotations(column.table.id, rowId, Seq(column))
    } yield cellLevelAnnotations
  }

  def retrieveCell(
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      isInternalCall: Boolean = false
  )(implicit user: TableauxUser): Future[Cell[Any]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(ViewCellValue, ComparisonObjects(table, column), isInternalCall)
      cell <- retrieveCell(column, rowId, isInternalCall)
    } yield cell
  }

  private def retrieveCell(column: ColumnType[_], rowId: RowId, isInternalCall: Boolean)(
      implicit user: TableauxUser
  ): Future[Cell[Any]] = {

    // In case of a ConcatColumn we need to retrieve the
    // other values too, so the ConcatColumn can be build.
    val columns = column match {
      case c: ConcatColumn => c.columns.+:(c)
      case c: GroupColumn => c.columns.+:(c)
      case c: StatusColumn => c.columns.+:(c)
      case _ => Seq(column)
    }

    for {
      rowLevelAnnotationsCache <- eventClient.retrieveRowLevelAnnotations(column.table.id, rowId)
      valueCache <- eventClient.retrieveCellValue(column.table.id, column.id, rowId)

      value <- valueCache match {
        case Some(obj) => {
          // Cache hit
          Future.successful(obj)
        }
        case None =>
          // Cache miss
          for {
            rowSeq <- column match {
              case _: AttachmentColumn =>
                // Special case for AttachmentColumns
                // Can't be handled by RowModel
                for {
                  attachments <- attachmentModel.retrieveAll(column.table.id, column.id, rowId)
                  // Dummy value for rowLevelAnnotations, rowPermissions and cellLevelAnnotations
                  // are okay here, because we only want to get a cell's value
                } yield Seq(Row(column.table, rowId, null, null, null, Seq(attachments)))

              case _ =>
                for {
                  rawRows <- retrieveRowModel.retrieve(column.table.id, rowId, columns)
                  mappedRows <- mapRawRows(column.table, columns, Seq(rawRows))
                } yield mappedRows
            }
          } yield {
            // Because we only want a cell's value other
            // potential rows and columns can be ignored.
            val value = rowSeq.head.values.head

            // fire-and-forget don't need to wait for this to return
            eventClient.setCellValue(column.table.id, column.id, rowId, value)
            value
          }
      }

      resultValue <-
        if (config.isRowPermissionCheckEnabled) {
          for {
            rowPermissions <- retrieveRowModel.retrieveRowPermissions(column.table.id, rowId)
            _ <- roleModel.checkAuthorization(ViewRow, ComparisonObjects(rowPermissions), isInternalCall)
            filteredValue <- removeUnauthorizedLinkAndConcatValues(column, value, true)
          } yield filteredValue
        } else {
          Future.successful(value)
        }

      rowLevelAnnotations <- rowLevelAnnotationsCache match {
        case Some(annotations) => {
          Future.successful(annotations)
        }
        case None => {
          for {
            (rowLevelAnnotations, _, _) <- retrieveRowModel.retrieveAnnotations(column.table.id, rowId, Seq(column))
          } yield {
            // fire-and-forget don't need to wait for this to return
            eventClient.setRowLevelAnnotations(column.table.id, rowId, rowLevelAnnotations)
            rowLevelAnnotations
          }

        }
      }
    } yield {
      Cell(column, rowId, resultValue, rowLevelAnnotations)
    }
  }

  private def removeUnauthorizedForeignValuesFromRows(
      columns: Seq[ColumnType[_]],
      rows: Seq[Row],
      shouldHideValuesByRowPermissions: Boolean = true
  )(implicit user: TableauxUser): Future[Seq[Row]] = {
    Future.sequence(rows map {
      case row => removeUnauthorizedLinkAndConcatValuesFromRow(columns, row, shouldHideValuesByRowPermissions)
    })
  }

  private def removeUnauthorizedLinkAndConcatValuesFromRow(
      columns: Seq[ColumnType[_]],
      row: Row,
      shouldHideValuesByRowPermissions: Boolean = true
  )(implicit user: TableauxUser): Future[Row] = {
    val rowValues = row.values
    val rowLevelAnnotations = row.rowLevelAnnotations
    val rowPermissions = row.rowPermissions
    val cellLevelAnnotations = row.cellLevelAnnotations
    val table = row.table
    val id = row.id

    for {
      newRowValues <-
        removeUnauthorizedLinkAndConcatValuesFromRowValues(columns, rowValues, shouldHideValuesByRowPermissions)
    } yield {
      Row(table, id, rowLevelAnnotations, rowPermissions, cellLevelAnnotations, newRowValues)
    }
  }

  private def removeUnauthorizedLinkAndConcatValuesFromRowValues(
      columns: Seq[ColumnType[_]],
      rowValues: Seq[_],
      shouldHideValuesByRowPermissions: Boolean = false
  )(implicit user: TableauxUser): Future[Seq[_]] = {
    Future.sequence(columns zip rowValues map {
      case (column, rowValue) =>
        removeUnauthorizedLinkAndConcatValues(column, rowValue, shouldHideValuesByRowPermissions)
    })
  }

  // Recursively traverses nested Link and Concat values
  // to filter out foreign row values which the user is not permitted to view.
  private def removeUnauthorizedLinkAndConcatValues(
      column: ColumnType[_],
      value: Any,
      shouldHideValuesByRowPermissions: Boolean = false
  )(implicit user: TableauxUser): Future[Any] = {
    (column, value) match {
      case (c: LinkColumn, linkSeq) => {
        val foreignTable = c.to.table
        val links: Seq[JsonObject] = (linkSeq match {
          case l: Seq[_] => l
          case l: JsonArray => l.asScala.toSeq
          case _ => throw new Exception("Links were not a Sequence or JsonArray")
        }).map(_.asInstanceOf[JsonObject])
        val linksRowIds = links.map(_.getLong("id").longValue())
        val linkedRowsPermissionsFuture: Future[Seq[RowPermissions]] =
          retrieveRowModel.retrieveRowsPermissions(foreignTable.id, linksRowIds)

        val checkAuthorizationAndMutateValues: (Seq[(JsonObject, RowPermissions)]) => Future[Seq[JsonObject]] =
          (rowIdsWithPermissions) => {
            val mutatedValuesFutures = rowIdsWithPermissions.map(tup => {
              val (link, rowPermissions) = tup
              val linkRowId = link.getLong("id").longValue()
              canUserViewRow(foreignTable, linkRowId, rowPermissions) flatMap {

                val buildReturnJson: (Option[Any], Boolean) => JsonObject = (valueOpt, userCanView) => {
                  if (shouldHideValuesByRowPermissions && !userCanView) {
                    Json.obj(
                      "id" -> linkRowId,
                      "hiddenByRowPermissions" -> true
                    )
                  } else {
                    Json.obj(
                      "id" -> linkRowId,
                      "value" -> valueOpt.getOrElse(null)
                    )
                  }
                }

                _ match {
                  case true => {
                    for {
                      mutatedValues <- c.to match {
                        case concatColumn: ConcatColumn => {
                          removeUnauthorizedLinkAndConcatValuesFromRowValues(
                            concatColumn.columns,
                            link.getJsonArray("value").asScala.toSeq
                          )
                        }
                        case anyColumn: ColumnType[_] => {
                          removeUnauthorizedLinkAndConcatValues(anyColumn, link.getValue("value"))
                        }

                      }
                    } yield {
                      buildReturnJson(Some(mutatedValues), true)

                    }
                  }
                  case false => {
                    Future.successful(buildReturnJson(None, false))
                  }
                }
              }
            })
            Future.sequence(mutatedValuesFutures)
          }

        for {
          linkedRows <- linkedRowsPermissionsFuture
          mutatedValues <- checkAuthorizationAndMutateValues(links zip linkedRows)
        } yield {
          mutatedValues
        }
      }
      case (c: ConcatColumn, concats) => {
        val concatSeq: Seq[_] = concats match {
          case c: Seq[_] => c
          case c: JsonArray => c.asScala.toSeq
        }
        removeUnauthorizedLinkAndConcatValuesFromRowValues(c.columns, concatSeq, true)
      }
      case (c, value) => {
        Future.successful(value)
      }
    }
  }

  def retrieveRow(table: Table, rowId: RowId)(implicit user: TableauxUser): Future[Row] = {
    for {
      columns <- retrieveColumns(table)
      filteredColumns = roleModel
        .filterDomainObjects[ColumnType[_]](
          ViewCellValue,
          columns,
          ComparisonObjects(table),
          isInternalCall = false
        )
      row <- retrieveRow(table, filteredColumns, rowId)
      resultRow <-
        if (config.isRowPermissionCheckEnabled) {
          for {
            _ <- roleModel.checkAuthorization(ViewRow, ComparisonObjects(row.rowPermissions), isInternalCall = false)
            mutatedRow <- removeUnauthorizedLinkAndConcatValuesFromRow(filteredColumns, row)
          } yield mutatedRow
        } else {
          Future.successful(row)
        }
    } yield resultRow
  }

  private def retrieveRow(table: Table, columns: Seq[ColumnType[_]], rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Row] = {
    for {
      rawRow <- retrieveRowModel.retrieve(table.id, rowId, columns)
      rowSeq <- mapRawRows(table, columns, Seq(rawRow))
    } yield rowSeq.head
  }

  def retrieveForeignRows(
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      finalFlagOpt: Option[Boolean],
      archivedFlagOpt: Option[Boolean],
      pagination: Pagination
  )(implicit user: TableauxUser): Future[RowSeq] = {
    for {
      linkColumn <- retrieveColumn(table, columnId).flatMap({
        case linkColumn: LinkColumn => Future.successful(linkColumn)
        case column => Future.failed(WrongColumnKindException(column, classOf[LinkColumn]))
      })

      foreignTable = linkColumn.to.table

      representingColumns <- retrieveColumns(foreignTable, isInternalCall = true)
        .map({ foreignColumns =>
          // we only need the first/representing column
          val firstForeignColumn = foreignColumns.head

          // In case of a ConcatColumn we need to retrieve the
          // other values too, so the ConcatColumn can be built.
          firstForeignColumn match {
            case c: ConcatColumn =>
              c.columns.+:(c)
            case _ =>
              Seq(firstForeignColumn)
          }
        })

      (_, linkDirection, _) <- structureModel.columnStruc.retrieveLinkInformation(table, linkColumn.id)
      totalSize <- retrieveRowModel.sizeForeign(linkColumn, rowId, linkDirection, finalFlagOpt, archivedFlagOpt)
      rawRows <- retrieveRowModel.retrieveForeign(
        linkColumn,
        rowId,
        representingColumns,
        finalFlagOpt,
        archivedFlagOpt,
        pagination,
        linkDirection
      )
      rowSeq <- mapRawRows(table, representingColumns, rawRows)
      resultRowSeq <-
        if (config.isRowPermissionCheckEnabled) {
          removeUnauthorizedForeignValuesFromRows(representingColumns, rowSeq)
        } else {
          Future.successful(rowSeq)
        }
    } yield {
      val filteredForeignRows = roleModel.filterDomainObjects(ViewRow, resultRowSeq, ComparisonObjects(), false)
      val rowsSeq = RowSeq(filteredForeignRows, Page(pagination, Some(totalSize)))
      copyFirstColumnOfRowsSeq(rowsSeq)
    }
  }

  private def copyFirstColumnOfRowsSeq(rowsSeq: RowSeq): RowSeq = {
    rowsSeq.copy(rows = rowsSeq.rows.map(row => row.copy(values = row.values.take(1))))
  }

  def retrieveRows(
      table: Table,
      finalFlagOpt: Option[Boolean],
      archivedFlagOpt: Option[Boolean],
      pagination: Pagination
  )(implicit user: TableauxUser): Future[RowSeq] = {
    // TODO: if the table is of kind UnionTable, we need to retrieve the rows from the underlying original tables in a completely different way
    if (table.tableType == UnionTable) {
      Future.successful(RowSeq(Seq.empty, Page(pagination, Some(0))))
    } else {
      for {
        columns <- retrieveColumns(table)
        filteredColumns = roleModel
          .filterDomainObjects[ColumnType[_]](
            ViewCellValue,
            columns,
            ComparisonObjects(table),
            isInternalCall = false
          )
        rowSeq <- retrieveRows(table, filteredColumns, finalFlagOpt, archivedFlagOpt, pagination)
        resultRows <-
          if (config.isRowPermissionCheckEnabled) {
            removeUnauthorizedForeignValuesFromRows(filteredColumns, rowSeq.rows)
          } else {
            Future.successful(rowSeq.rows)
          }
      } yield {
        val filteredRows = roleModel.filterDomainObjects(ViewRow, resultRows, ComparisonObjects(), false)
        RowSeq(filteredRows, rowSeq.page)
      }
    }
  }

  def retrieveRows(
      table: Table,
      columnId: ColumnId,
      finalFlagOpt: Option[Boolean],
      archivedFlagOpt: Option[Boolean],
      pagination: Pagination
  )(
      implicit user: TableauxUser
  ): Future[RowSeq] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- roleModel.checkAuthorization(ViewCellValue, ComparisonObjects(table, column))

      // In case of a ConcatColumn we need to retrieve the
      // other values too, so the ConcatColumn can be build.
      columns = column match {
        case c: ConcatenateColumn => c.columns.+:(c)
        case _ => Seq(column)
      }

      rowsSeq <- retrieveRows(table, columns, finalFlagOpt, archivedFlagOpt, pagination)
    } yield {
      copyFirstColumnOfRowsSeq(rowsSeq)
    }
  }

  private def retrieveRows(
      table: Table,
      columns: Seq[ColumnType[_]],
      finalFlagOpt: Option[Boolean],
      archivedFlagOpt: Option[Boolean],
      pagination: Pagination
  )(
      implicit user: TableauxUser
  ): Future[RowSeq] = {
    for {
      totalSize <- retrieveRowModel.size(table.id, finalFlagOpt, archivedFlagOpt)
      rawRows <- retrieveRowModel.retrieveAll(table.id, columns, finalFlagOpt, archivedFlagOpt, pagination)
      rowSeq <- mapRawRows(table, columns, rawRows)
    } yield {
      RowSeq(rowSeq, Page(pagination, Some(totalSize)))
    }
  }

  def duplicateRow(table: Table, rowId: RowId, options: Option[DuplicateRowOptions])(implicit
  user: TableauxUser): Future[Row] = {
    val isConstrainedLink = (link: LinkColumn) => link.linkDirection.constraint.cardinality.from > 0
    val shouldAnnotateSkipped = options.fold(false)(_.annotateSkipped)
    val shouldSkipConstrained = options.fold(false)(_.skipConstrainedFrom)
    val specificColumns = options.flatMap(_.columnIds)
    def canBeDuplicated(col: ColumnType[_]): Boolean = col match {
      case _: ConcatColumn => false
      case _: GroupColumn => false
      case _ => true
    }
    def isIn(xs: Seq[ColumnId]): ((ColumnType[_]) => Boolean) = {
      val lookup = xs.toSet
      (y: ColumnType[_]) => lookup contains y.id
    }
    for {
      _ <- roleModel.checkAuthorization(CreateRow, ComparisonObjects(table))
      allColumns <- retrieveColumns(table)
      columnsToDuplicate = allColumns
        .filter(canBeDuplicated)
        .filter({
          case link: LinkColumn if shouldSkipConstrained => !isConstrainedLink(link)
          case _ => true
        })
        .filter(specificColumns match {
          case Some(columnsToKeep) => isIn(columnsToKeep)
          case _ => (_: Any) => true
        })
      skippedColumns = allColumns.filter(col => !isIn(columnsToDuplicate.map(_.id))(col))

      // Retrieve row without skipped columns
      row <- retrieveRow(table, columnsToDuplicate, rowId)
      rowValues = row.values

      // First create a empty row
      duplicatedRowId <- createRowModel.createRow(table, Seq.empty, Option(row.rowPermissions.value))

      // Fill the row with life
      _ <- updateRowModel
        .updateRow(table, duplicatedRowId, columnsToDuplicate.zip(rowValues))
        // If this fails delete the row, cleanup time
        .recoverWith({
          case NonFatal(ex) =>
            deleteRow(table, duplicatedRowId)
              .flatMap(_ => Future.failed(ex))
        })

      _ <- createHistoryModel.createRow(table, duplicatedRowId, Option(row.rowPermissions.value))
      _ <- createHistoryModel.createCells(table, rowId, columnsToDuplicate.zip(rowValues))
      _ <-
        if (shouldAnnotateSkipped) {
          Future.sequence(skippedColumns.filter(canBeDuplicated).map(col =>
            addCellAnnotation(col, duplicatedRowId, Seq.empty, CellAnnotationType(CellAnnotationType.FLAG), "check-me")
          ))
        } else Future.successful(())

      // Retrieve duplicated row with all columns
      duplicatedRow <- retrieveRow(table, duplicatedRowId)
    } yield duplicatedRow
  }

  private def canUserViewRow(
      table: Table,
      rowId: RowId,
      permissions: RowPermissions
  )(implicit user: TableauxUser): Future[Boolean] = {
    roleModel.checkAuthorization(
      ViewRow,
      ComparisonObjects(permissions),
      isInternalCall = false,
      shouldLog = false
    ) transform {
      case Success(_) => Success(true)
      case Failure(_) => Success(false)
    }
  }

  private def mapRawRows(table: Table, columns: Seq[ColumnType[_]], rawRows: Seq[RawRow])(
      implicit user: TableauxUser
  ): Future[Seq[Row]] = {

    /**
      * Fetches ConcatColumn values for linked rows
      */
    def fetchConcatValuesForLinkedRows(
        concatenateColumn: ConcatenateColumn,
        linkedRows: JsonArray
    ): Future[List[JsonObject]] = {
      import scala.collection.JavaConverters._

      // Iterate over each linked row and
      // replace json's value with ConcatColumn value
      linkedRows.asScala.map(_.asInstanceOf[JsonObject]).foldLeft(Future.successful(List.empty[JsonObject])) {
        case (futureList, linkedRow: JsonObject) =>
          // ConcatColumn's value is always a
          // json array with the linked row ids
          val rowId = linkedRow.getLong("id").longValue()

          for {
            list <- futureList
            cell <- retrieveCell(concatenateColumn, rowId, true)
          } yield {
            val cellJson = cell.getJson
            list ++ List(Json.obj("id" -> rowId).mergeIn(cellJson))
          }
      }
    }

    def fetchValuesForStatusColumn(
        concatenateColumn: ConcatenateColumn,
        rowId: RowId
    ): Future[Map[ColumnId, (ColumnType[_], Any)]] = {
      val columns = concatenateColumn.columns
      for {
        row <- retrieveRow(concatenateColumn.table, columns, rowId)
      } yield columns
        .zip(row.values)
        .foldLeft(Map[ColumnId, (ColumnType[_], Any)]())({
          case (acc, (col, remVal)) => acc + (col.id -> (col, remVal))
        })
    }

    def calcStatusValue(rules: JsonArray, columnsWithValues: Map[ColumnId, (ColumnType[_], Any)]): Seq[Boolean] = {

      def calcValue(condition: JsonObject)(implicit user: TableauxUser): Boolean = {

        val compositionFunction = condition.getString("composition") match {
          case "OR" =>
            (acc: Boolean, value: Boolean) =>
              acc || value
          case "AND" =>
            (acc: Boolean, value: Boolean) =>
              acc && value
        }

        val values = condition.getJsonArray("values")

        asSeqOf[JsonObject](values)
          .map(value => {
            if (value.containsKey("values")) {
              calcValue(value)
            } else {
              val columnId: ColumnId = value.getLong("column").asInstanceOf[ColumnId]
              val (column, columnValue) = columnsWithValues(columnId)

              val operatorFunction = value.getString("operator") match {
                case "NOT" =>
                  (a: Any, b: Any) =>
                    a != b
                case _ =>
                  (a: Any, b: Any) =>
                    a == b
              }

              val compareValue = value.getValue("value")
              operatorFunction(columnValue, compareValue)
            }
          })
          .reduceLeft(compositionFunction)
      }

      asSeqOf[JsonObject](rules).map(rule => {
        val conditions = rule.getJsonObject("conditions")
        calcValue(conditions)
      })

    }

    Future.sequence(rawRows.map({
      case RawRow(rowId, rowLevelFlags, rowPermissions, cellLevelFlags, rawValues) => {
        for {
          // Chain post-processing RawRows

          // Fetch values for RawRow which couldn't be fetched by SQL
          columnsWithFetchedValues <-
            Future.sequence(
              columns
                .zip(rawValues)
                .map({
                  case (c: LinkColumn, array: JsonArray) if c.to.isInstanceOf[ConcatenateColumn] =>
                    // Fetch linked values of each linked row
                    fetchConcatValuesForLinkedRows(c.to.asInstanceOf[ConcatenateColumn], array)
                      .map(cellValue => (c, cellValue))

                  case (c: StatusColumn, value) =>
                    for {
                      dependentColumnValues <- fetchValuesForStatusColumn(c.asInstanceOf[ConcatenateColumn], rowId)
                      statusValue = calcStatusValue(c.rules, dependentColumnValues)
                    } yield { (c, statusValue) }

                  case (c: AttachmentColumn, _) =>
                    // AttachmentColumns are fetched via AttachmentModel
                    retrieveCell(c, rowId, true)
                      .map(cell => (c, cell.value))

                  case (c, value) =>
                    // All other column types were already fetched by RetrieveRowModel
                    Future.successful((c, value))
                })
            )

          // Generate values for GroupColumn && ConcatColumn
          columnsWithPostProcessedValues = columnsWithFetchedValues.map({
            case (c: StatusColumn, value) => value

            case (c: ConcatenateColumn, _) =>
              // Post-process GroupColumn && ConcatColumn, concatenate their values
              columnsWithFetchedValues
                .filter({
                  case (column: ColumnType[_], _) => c.columns.exists(_.id == column.id)
                })
                .map({
                  case (_, value) => value
                })

            case (_, value) =>
              // Post-processing is only needed for ConcatColumn and GroupColumn
              value
          })
        } yield {
          Row(table, rowId, rowLevelFlags, rowPermissions, cellLevelFlags, columnsWithPostProcessedValues)
        }
      }
    }))
  }

  def retrieveColumnValues(table: Table, columnId: ColumnId, langtagOpt: Option[String])(
      implicit user: TableauxUser
  ): Future[Seq[String]] = {
    for {
      shortTextColumn <- retrieveColumn(table, columnId).flatMap({
        case shortTextColumn: ShortTextColumn => Future.successful(shortTextColumn)
        case column => Future.failed(WrongColumnKindException(column, classOf[ShortTextColumn]))
      })

      _ <- roleModel.checkAuthorization(ViewCellValue, ComparisonObjects(table, shortTextColumn))
      values <- retrieveRowModel.retrieveColumnValues(shortTextColumn, langtagOpt)
    } yield values
  }

  def retrieveTotalSize(
      table: Table,
      finalFlagOpt: Option[Boolean] = None,
      archivedFlagOpt: Option[Boolean] = None
  ): Future[Long] = {
    retrieveRowModel.size(table.id, finalFlagOpt, archivedFlagOpt)
  }

  def retrieveCellHistory(
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  )(implicit user: TableauxUser): Future[Seq[History]] = {
    for {
      _ <-
        if (config.isRowPermissionCheckEnabled) {
          for {
            rowPermissions <- retrieveRowModel.retrieveRowPermissions(table.id, rowId)
            _ <- roleModel.checkAuthorization(ViewRow, ComparisonObjects(rowPermissions))
          } yield ()
        } else {
          Future.successful(())
        }
      column <- retrieveColumn(table, columnId)
      _ <- checkColumnTypeForLangtag(column, langtagOpt)
      _ <- roleModel.checkAuthorization(ViewCellValue, ComparisonObjects(table, column))
      cellHistorySeq <- retrieveHistoryModel.retrieveCell(table, column, rowId, langtagOpt, typeOpt, includeDeleted)
    } yield cellHistorySeq
  }

  def retrieveColumnHistory(
      table: Table,
      columnId: ColumnId,
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  )(implicit user: TableauxUser): Future[Seq[History]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- checkColumnTypeForLangtag(column, langtagOpt)
      _ <- roleModel.checkAuthorization(ViewCellValue, ComparisonObjects(table, column))
      columnHistorySeq <- retrieveHistoryModel.retrieveColumn(table, column, langtagOpt, typeOpt, includeDeleted)
    } yield columnHistorySeq
  }

  def retrieveRowHistory(
      table: Table,
      rowId: RowId,
      langtagOpt: Option[String],
      typeOpt: Option[String],
      includeDeleted: Boolean
  )(implicit user: TableauxUser): Future[Seq[History]] = {
    for {
      _ <-
        if (config.isRowPermissionCheckEnabled) {
          for {
            rowPermissions <- retrieveRowModel.retrieveRowPermissions(table.id, rowId)
            _ <- roleModel.checkAuthorization(ViewRow, ComparisonObjects(rowPermissions))
          } yield ()
        } else {
          Future.successful(())
        }
      columns <- retrieveColumns(table)
      cellHistorySeq <- retrieveHistoryModel.retrieveRow(table, rowId, langtagOpt, typeOpt, includeDeleted)
      filteredCellHistorySeq = filterCellHistoriesForColumns(cellHistorySeq, columns)
    } yield filteredCellHistorySeq
  }

  def retrieveTableHistory(table: Table, langtagOpt: Option[String], typeOpt: Option[String], includeDeleted: Boolean)(
      implicit user: TableauxUser
  ): Future[Seq[History]] = {
    for {
      columns <- retrieveColumns(table)
      cellHistorySeq <- retrieveHistoryModel.retrieveTable(table, langtagOpt, typeOpt, includeDeleted)
      filteredCellHistorySeq = filterCellHistoriesForColumns(cellHistorySeq, columns)
    } yield filteredCellHistorySeq
  }

  private def filterCellHistoriesForColumns(cellHistorySeq: Seq[History], columns: Seq[ColumnType[_]]) = {
    cellHistorySeq.filter(history => {
      val columnIds: Seq[ColumnId] = columns.map(_.id)
      history.columnIdOpt.forall(historyColumnId => columnIds.contains(historyColumnId))
    })
  }

  private def checkColumnTypeForLangtag[A](column: ColumnType[_], langtagOpt: Option[String]): Future[Unit] = {
    (column.languageType, langtagOpt) match {
      case (LanguageNeutral, Some(_)) =>
        Future.failed(
          InvalidRequestException(
            "History values filtered by langtags can only be retrieved from multi-language columns"
          )
        )
      case (_, _) => Future.successful(())

    }
  }

  def addRowPermissions(table: Table, rowId: RowId, rowPermissions: RowPermissionSeq)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      row <- retrieveRow(table, rowId)
      newRowPermissionsOpt <- updateRowModel.addRowPermissions(table, row, rowPermissions)
      // ensure that user can view the row with the new permissions, otherwise he is not allowed to it
      _ <- roleModel.checkAuthorization(ViewRow, ComparisonObjects(RowPermissions(rowPermissions)))
      _ <- createHistoryModel.updateRowPermission(table, rowId, newRowPermissionsOpt)
    } yield ()
  }

  def deleteRowPermissions(table: Table, rowId: RowId)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      row <- retrieveRow(table, rowId)
      newRowPermissionsOpt <- updateRowModel.deleteRowPermissions(table, row)
      _ <- createHistoryModel.updateRowPermission(table, rowId, newRowPermissionsOpt)
    } yield ()
  }

  def replaceRowPermissions(table: Table, rowId: RowId, rowPermissions: RowPermissionSeq)(
      implicit user: TableauxUser
  ): Future[Unit] = {
    for {
      row <- retrieveRow(table, rowId)
      // ensure that user can view the row with the new permissions, otherwise he is not allowed to it
      _ <- roleModel.checkAuthorization(ViewRow, ComparisonObjects(RowPermissions(rowPermissions)))
      newRowPermissionsOpt <- updateRowModel.replaceRowPermissions(table, row, rowPermissions)
      _ <- createHistoryModel.updateRowPermission(table, rowId, newRowPermissionsOpt)
    } yield ()
  }

  def retrieveFileDependentRows(uuid: UUID)(
      implicit user: TableauxUser
  ): Future[FileDependentRowsSeq] = {
    logger.info(s"retrieveFileDependentRows $uuid")

    for {
      cellsForFiles <- attachmentModel.retrieveCells(uuid)
      result <- {
        val futures = cellsForFiles.sortBy({
          case (tableId, columnId, rowId) => (tableId, columnId, rowId)
        }).map({
          case (tableId, columnId, rowId) =>
            for {
              table <- retrieveTable(tableId, isInternalCall = true)
              columns <- retrieveColumns(table, isInternalCall = true)
              cell <- retrieveCell(columns.head, rowId, isInternalCall = true)
            } yield {
              val column = columns.find(_.id == columnId).getOrElse(columns.head)
              val row = Row(
                table,
                rowId,
                RowLevelAnnotations(false, false),
                RowPermissions(Json.arr()),
                CellLevelAnnotations(Seq(), Json.arr()),
                Seq(cell.value)
              )

              (table, columns.head, Seq(FileDependentRow(column, row)))
            }
        })

        Future.sequence(futures)
      }
    } yield {
      val objects = result
        .groupBy({ case (dependentTable, column, _) => (dependentTable, column) })
        .map({
          case ((groupedByTable, groupedByColumn), dependentRowInformation) =>
            (
              groupedByTable,
              groupedByColumn,
              dependentRowInformation
                .flatMap({
                  case (_, _, values) => values
                })
                .distinct
            )
        })
        .filter({
          case (_, _, values) => values.nonEmpty
        })
        .map({
          case (groupedByTable, groupedByColumn, dependentRows) =>
            FileDependentRows(groupedByTable, groupedByColumn, dependentRows)
        })
        .toSeq

      FileDependentRowsSeq(objects)
    }
  }
}
