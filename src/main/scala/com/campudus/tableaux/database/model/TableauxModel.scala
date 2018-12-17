package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{CreateRowModel, RetrieveRowModel, UpdateRowModel}
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.{
  ForbiddenException,
  InvalidRequestException,
  ShouldBeUniqueException,
  WrongColumnKindException
}
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object TableauxModel {
  type LinkId = Long
  type TableGroupId = Long
  type TableId = Long
  type ColumnId = Long
  type RowId = Long

  type Ordering = Long

  def apply(connection: DatabaseConnection, structureModel: StructureModel): TableauxModel = {
    new TableauxModel(connection, structureModel)
  }
}

/**
  * Needed because e.g. `TableauxController#createCompleteTable` and `TableauxController#retrieveCompleteTable`
  * need to call method from `StructureModel`.
  */
sealed trait StructureDelegateModel extends DatabaseQuery {

  import TableauxModel._

  protected val connection: DatabaseConnection

  protected val structureModel: StructureModel

  def createTable(name: String, hidden: Boolean): Future[Table] = {
    structureModel.tableStruc.create(name, hidden, None, List(), GenericTable, None)
  }

  def retrieveTable(tableId: TableId): Future[Table] = {
    structureModel.tableStruc.retrieve(tableId)
  }

  def retrieveTables(): Future[Seq[Table]] = {
    structureModel.tableStruc.retrieveAll()
  }

  def createColumns(table: Table, columns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = {
    structureModel.columnStruc.createColumns(table, columns)
  }

  def retrieveColumn(table: Table, columnId: ColumnId): Future[ColumnType[_]] = {
    structureModel.columnStruc.retrieve(table, columnId)
  }

  def retrieveColumns(table: Table): Future[Seq[ColumnType[_]]] = {
    structureModel.columnStruc.retrieveAll(table)
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
    override protected[this] val structureModel: StructureModel
) extends DatabaseQuery
    with StructureDelegateModel {

  import TableauxModel._

  val retrieveRowModel = new RetrieveRowModel(connection)
  val createRowModel = new CreateRowModel(connection)
  val updateRowModel = new UpdateRowModel(connection)

  val attachmentModel = AttachmentModel(connection)
  val retrieveHistoryModel = RetrieveHistoryModel(connection)
  val createHistoryModel = CreateHistoryModel(this, connection)
  val createInitialHistoryModel = CreateInitialHistoryModel(this, connection)

  def retrieveDependentRows(table: Table, rowId: RowId): Future[DependentRowsSeq] = {

    def selectDependentRows(linkId: LinkId, linkDirection: LinkDirection) = {
      s"SELECT ${linkDirection.toSql} FROM link_table_$linkId WHERE ${linkDirection.fromSql} = ?"
    }

    for {
      links <- retrieveDependentLinks(table)
      result <- {
        val futures = links.map({
          case (linkId, linkDirection) =>
            connection
              .query(selectDependentRows(linkId, linkDirection), Json.arr(rowId))
              .map(result => resultObjectToJsonArray(result).map(_.getLong(0).longValue()))
              .map(dependentRows => (linkDirection.to, dependentRows))
              .flatMap({
                case (tableId, rows) =>
                  for {
                    table <- retrieveTable(tableId)
                    columns <- retrieveColumns(table)
                    rowObjects <- Future.sequence(rows.map({ rowId =>
                      {
                        retrieveCell(columns.head, rowId)
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
            (groupedByTable,
             groupedByColumn,
             dependentRowInformation
               .flatMap({
                 case (_, _, values) => values
               })
               .distinct)
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

  def deleteRow(table: Table, rowId: RowId): Future[EmptyObject] = {
    for {
      specialColumns <- retrieveColumns(table).map(_.filter({
        case _: AttachmentColumn => true
        case c: LinkColumn if c.linkDirection.constraint.deleteCascade => true
        case _ => false
      }))

      // Clear special cells before delete.
      // For example AttachmentColumns will
      // not be deleted by DELETE CASCADE.
      // clearing LinkColumn will eventually trigger delete cascade
      _ <- updateRowModel.clearRow(table, rowId, specialColumns, deleteRow)

      _ <- updateRowModel.deleteRow(table.id, rowId)

      // invalidate row
      _ <- CacheClient(this.connection).invalidateRow(table.id, rowId)
    } yield EmptyObject()
  }

  def createRow(table: Table): Future[Row] = {
    for {
      rowId <- createRowModel.createRow(table, Seq.empty)
      row <- retrieveRow(table, rowId)
    } yield row
  }

  def createRows(table: Table, rows: Seq[Seq[(ColumnId, Any)]]): Future[RowSeq] = {
    for {
      columns <- retrieveColumns(table)
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

            rowId <- createRowModel.createRow(table, columnValuePairs)
            _ <- createHistoryModel.create(table, rowId, columnValuePairs)

            newRow <- retrieveRow(table, columns, rowId)
          } yield {
            rows ++ Seq(newRow)
          }
        }
      }
      }
    } yield RowSeq(rows)
  }

  def addCellAnnotation(
      column: ColumnType[_],
      rowId: RowId,
      langtags: Seq[String],
      annotationType: CellAnnotationType,
      value: String
  ): Future[CellLevelAnnotation] = {
    for {
      (uuid, mergedLangtags, createdAt) <- updateRowModel.addOrMergeCellAnnotation(
        column,
        rowId,
        langtags,
        annotationType,
        value
      )
    } yield CellLevelAnnotation(uuid, annotationType, mergedLangtags, value, createdAt)
  }

  def deleteCellAnnotation(column: ColumnType[_], rowId: RowId, uuid: UUID): Future[Unit] = {
    for {
      _ <- updateRowModel.deleteCellAnnotation(column, rowId, uuid)
    } yield ()
  }

  def deleteCellAnnotation(column: ColumnType[_], rowId: RowId, uuid: UUID, langtag: String): Future[Unit] = {
    for {
      _ <- updateRowModel.deleteCellAnnotation(column, rowId, uuid, langtag)
    } yield ()
  }

  def updateRowAnnotations(table: Table, rowId: RowId, finalFlag: Option[Boolean]): Future[Row] = {
    for {
      _ <- updateRowModel.updateRowAnnotations(table.id, rowId, finalFlag)
      row <- retrieveRow(table, rowId)
    } yield row
  }

  def updateRowsAnnotations(table: Table, finalFlag: Option[Boolean]): Future[Unit] = {
    for {
      _ <- updateRowModel.updateRowsAnnotations(table.id, finalFlag)
    } yield ()
  }

  def retrieveTableWithCellAnnotations(table: Table): Future[TableWithCellAnnotations] = {
    retrieveTablesWithCellAnnotations(Seq(table)).map({ annotations =>
      annotations.headOption.getOrElse(TableWithCellAnnotations(table, Map.empty))
    })
  }

  def retrieveTablesWithCellAnnotations(tables: Seq[Table]): Future[Seq[TableWithCellAnnotations]] = {
    retrieveRowModel.retrieveTablesWithCellAnnotations(tables)
  }

  def retrieveTablesWithCellAnnotationCount(tables: Seq[Table]): Future[Seq[TableWithCellAnnotationCount]] = {
    val tableIds = tables.map({ case Table(id, _, _, _, _, _, _) => id })

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

  def deleteLink(table: Table, columnId: ColumnId, rowId: RowId, toId: RowId): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)

      _ <- column match {
        case linkColumn: LinkColumn => {
          for {
            _ <- createInitialHistoryModel.createHistoryIfNotExists(table, rowId, Seq((column, Seq(toId))))
            _ <- updateRowModel.deleteLink(table, linkColumn, rowId, toId, deleteRow)
            _ <- createHistoryModel.deleteLinks(table, rowId, Seq((linkColumn, Seq(toId))))
          } yield Future.successful(())
        }
        case _ => Future.failed(WrongColumnKindException(column, classOf[LinkColumn]))
      }

      _ <- invalidateCellAndDependentColumns(column, rowId)

      updatedCell <- retrieveCell(column, rowId)
    } yield updatedCell
  }

  def updateCellLinkOrder(
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      toId: RowId,
      locationType: LocationType
  ): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)

      _ <- column match {
        case linkColumn: LinkColumn => {
          for {
//            _ <- createInitialHistoryModel.createHistoryLinkOrderIfNotExists(table, linkColumn, rowId)
            _ <- createInitialHistoryModel.createLinksInit(table, rowId, Seq((linkColumn, Seq.empty[RowId])))
            _ <- updateRowModel.updateLinkOrder(table, linkColumn, rowId, toId, locationType)
            _ <- invalidateCellAndDependentColumns(column, rowId)
            _ <- createHistoryModel.updateLinkOrder(table, linkColumn, rowId)
          } yield Future.successful(())
        }
        case _ => Future.failed(WrongColumnKindException(column, classOf[LinkColumn]))
      }

      updatedCell <- retrieveCell(column, rowId)
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

  private def updateOrReplaceValue[A](
      table: Table,
      columnId: ColumnId,
      rowId: RowId,
      value: A,
      replace: Boolean = false
  ): Future[Cell[_]] = {
    for {
      _ <- checkForSettingsTable(table, columnId, "can't update key cell of a settings table")

      column <- retrieveColumn(table, columnId)
      _ <- checkValueTypeForColumn(column, value)

      _ <- if (replace) {
        updateRowModel.clearRowWithValues(table, rowId, Seq((column, value)), deleteRow)
      } else {
        Future.successful(())
      }

      _ <- createInitialHistoryModel.createHistoryIfNotExists(table, rowId, Seq((column, value)))
      _ <- updateRowModel.updateRow(table, rowId, Seq((column, value)))
      _ <- invalidateCellAndDependentColumns(column, rowId)
      _ <- createHistoryModel.create(table, rowId, Seq((column, value)), replace)

      changedCell <- retrieveCell(column, rowId)
    } yield changedCell
  }

  def updateCellValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] =
    updateOrReplaceValue(table, columnId, rowId, value)

  def replaceCellValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] =
    updateOrReplaceValue(table, columnId, rowId, value, replace = true)

  def clearCellValue(table: Table, columnId: ColumnId, rowId: RowId): Future[Cell[_]] = {
    for {
      _ <- checkForSettingsTable(table, columnId, "can't clear key cell of a settings table")

      column <- retrieveColumn(table, columnId)

      _ <- updateRowModel.clearRow(table, rowId, Seq(column), deleteRow)

      _ <- invalidateCellAndDependentColumns(column, rowId)

      clearedCell <- retrieveCell(column, rowId)
    } yield clearedCell
  }

  private def checkForSettingsTable[A](table: Table, columnId: ColumnId, exceptionMessage: String) = {
    (table.tableType, columnId) match {
      case (SettingsTable, 1 | 2) =>
        Future.failed(ForbiddenException(exceptionMessage, "cell"))
      case _ => Future.successful(())
    }
  }

  private def checkForDuplicateKey[A](table: Table, keyColumn: ColumnType[_], keyName: Option[Any]) = {
    retrieveRows(table, Seq(keyColumn), Pagination(None, None))
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
      case Some(key: String) if !key.isEmpty => {
        Future.successful(())
      }
      case _ => Future.failed(InvalidRequestException("Key must not be empty and a string in settings table"))
    }
  }

  private def invalidateCellAndDependentColumns(column: ColumnType[_], rowId: RowId): Future[Unit] = {

    def invalidateColumn: (TableId, ColumnId) => Future[_] =
      CacheClient(this.connection).invalidateColumn

    for {
      // invalidate the cell itself
      _ <- CacheClient(this.connection).invalidateCellValue(column.table.id, column.id, rowId)

      // invalidate the concat cell if column is an identifier
      _ <- if (column.identifier) {
        CacheClient(this.connection).invalidateCellValue(column.table.id, 0, rowId)
      } else {
        Future.successful(())
      }

      _ <- if (column.columnInformation.groupColumnIds.nonEmpty) {
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

        case DependentColumnInformation(tableId, columnId, _, true, groupColumnIds) =>
          // Only invalidate cache if depending link column is an identifier column
          // ... because only identifier link columns

          // Invalidate depending link column...
          val invalidateLinkColumn = invalidateColumn(tableId, columnId)
          // Invalidate the table's concat column - to be sure...
          val invalidateConcatColumn = invalidateColumn(tableId, 0)
          // Invalidate all depending group columns
          val invalidateGroupColumns =
            Future.sequence(groupColumnIds.map(invalidateColumn(tableId, _)))

          invalidateLinkColumn.zip(invalidateConcatColumn).zip(invalidateGroupColumns)

        case DependentColumnInformation(tableId, _, _, false, groupColumnIds) =>
          // If depending link column is no identifier column we only need to invalidate
          // ... group columns which dependent on link column

          // Invalidate all depending group columns
          val invalidateGroupColumns =
            Future.sequence(groupColumnIds.map(invalidateColumn(tableId, _)))

          invalidateGroupColumns
      }))
    } yield ()
  }

  def retrieveCell(table: Table, columnId: ColumnId, rowId: RowId): Future[Cell[Any]] = {
    for {
      column <- retrieveColumn(table, columnId)
      cell <- retrieveCell(column, rowId)
    } yield cell
  }

  private def retrieveCell(column: ColumnType[_], rowId: RowId): Future[Cell[Any]] = {

    // In case of a ConcatColumn we need to retrieve the
    // other values too, so the ConcatColumn can be build.
    val columns = column match {
      case c: ConcatColumn => c.columns.+:(c)
      case c: GroupColumn => c.columns.+:(c)
      case _ => Seq(column)
    }

    for {
      valueCache <- CacheClient(this.connection).retrieveCellValue(column.table.id, column.id, rowId)

      value <- valueCache match {
        case Some(obj) =>
          // Cache hit
          Future.successful(obj)
        case None =>
          // Cache miss
          for {
            rowSeq <- column match {
              case _: AttachmentColumn =>
                // Special case for AttachmentColumns
                // Can't be handled by RowModel
                for {
                  (rowLevelAnnotations, cellLevelAnnotations) <- retrieveRowModel
                    .retrieveAnnotations(column.table.id, rowId, Seq(column))
                  attachments <- attachmentModel.retrieveAll(column.table.id, column.id, rowId)
                } yield Seq(Row(column.table, rowId, rowLevelAnnotations, cellLevelAnnotations, Seq(attachments)))

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
            CacheClient(this.connection).setCellValue(column.table.id, column.id, rowId, value)

            value
          }
      }
    } yield Cell(column, rowId, value)
  }

  def retrieveRow(table: Table, rowId: RowId): Future[Row] = {
    for {
      columns <- retrieveColumns(table)
      row <- retrieveRow(table, columns, rowId)
    } yield row
  }

  private def retrieveRow(table: Table, columns: Seq[ColumnType[_]], rowId: RowId): Future[Row] = {
    for {
      rawRow <- retrieveRowModel.retrieve(table.id, rowId, columns)
      rowSeq <- mapRawRows(table, columns, Seq(rawRow))
    } yield rowSeq.head
  }

  def retrieveForeignRows(table: Table, columnId: ColumnId, rowId: RowId, pagination: Pagination): Future[RowSeq] = {
    for {
      linkColumn <- retrieveColumn(table, columnId).flatMap({
        case linkColumn: LinkColumn => Future.successful(linkColumn)
        case column => Future.failed(WrongColumnKindException(column, classOf[LinkColumn]))
      })

      foreignTable = linkColumn.to.table

      representingColumns <- retrieveColumns(foreignTable)
        .map({ foreignColumns =>
          // we only need the first/representing column
          val firstForeignColumn = foreignColumns.head

          // In case of a ConcatColumn we need to retrieve the
          // other values too, so the ConcatColumn can be build.
          firstForeignColumn match {
            case c: ConcatColumn =>
              c.columns.+:(c)
            case _ =>
              Seq(firstForeignColumn)
          }
        })

      totalSize <- retrieveRowModel.sizeForeign(linkColumn, rowId)
      rawRows <- retrieveRowModel.retrieveForeign(linkColumn, rowId, representingColumns, pagination)
      rowSeq <- mapRawRows(table, representingColumns, rawRows)
    } yield {
      val rowsSeq = RowSeq(rowSeq, Page(pagination, Some(totalSize)))
      rowsSeq.copy(rows = rowsSeq.rows.map(row => row.copy(values = row.values.take(1))))
    }
  }

  def retrieveRows(table: Table, pagination: Pagination): Future[RowSeq] = {
    for {
      columns <- retrieveColumns(table)
      rows <- retrieveRows(table, columns, pagination)
    } yield rows
  }

  def retrieveRows(table: Table, columnId: ColumnId, pagination: Pagination): Future[RowSeq] = {
    for {
      column <- retrieveColumn(table, columnId)

      // In case of a ConcatColumn we need to retrieve the
      // other values too, so the ConcatColumn can be build.
      columns = column match {
        case c: ConcatenateColumn => c.columns.+:(c)
        case _ => Seq(column)
      }

      rowsSeq <- retrieveRows(table, columns, pagination)
    } yield {
      rowsSeq.copy(rows = rowsSeq.rows.map(row => row.copy(values = row.values.take(1))))
    }
  }

  private def retrieveRows(table: Table, columns: Seq[ColumnType[_]], pagination: Pagination): Future[RowSeq] = {
    for {
      totalSize <- retrieveRowModel.size(table.id)
      rawRows <- retrieveRowModel.retrieveAll(table.id, columns, pagination)
      rowSeq <- mapRawRows(table, columns, rawRows)
    } yield RowSeq(rowSeq, Page(pagination, Some(totalSize)))
  }

  def duplicateRow(table: Table, rowId: RowId): Future[Row] = {
    for {
      columns <- retrieveColumns(table).map(_.filter({
        // ConcatColumn && GroupColumn can't be duplicated
        case _: ConcatColumn | _: GroupColumn => false
        // Other columns can be duplicated
        case _ => true
      }))

      // Retrieve row without ConcatColumn
      row <- retrieveRow(table, columns, rowId)
      rowValues = row.values

      // First create a empty row
      duplicatedRowId <- createRowModel.createRow(table, Seq.empty)

      // Fill the row with life
      _ <- updateRowModel
        .updateRow(table, duplicatedRowId, columns.zip(rowValues))
        // If this fails delete the row, cleanup time
        .recoverWith({
          case NonFatal(ex) =>
            deleteRow(table, duplicatedRowId)
              .flatMap(_ => Future.failed(ex))
        })

      _ <- createHistoryModel.create(table, rowId, columns.zip(rowValues))

      // Retrieve duplicated row with all columns
      duplicatedRow <- retrieveRow(table, duplicatedRowId)
    } yield duplicatedRow
  }

  private def mapRawRows(table: Table, columns: Seq[ColumnType[_]], rawRows: Seq[RawRow]): Future[Seq[Row]] = {

    /**
      * Fetches ConcatColumn values for
      * linked rows
      */
    def fetchConcatValuesForLinkedRows(concatnateColumn: ConcatenateColumn,
                                       linkedRows: JsonArray): Future[List[JsonObject]] = {
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
            cell <- retrieveCell(concatnateColumn, rowId)
          } yield list ++ List(Json.obj("id" -> rowId, "value" -> DomainObject.compatibilityGet(cell.value)))
      }
    }

    // post-process RawRows and transform them to Rows
    rawRows.foldLeft(Future.successful(List.empty[Row])) {
      case (rawRowsFuture, RawRow(rowId, rowLevelFlags, cellLevelFlags, rawValues)) =>
        for {
          // Chain post-processing RawRows
          list <- rawRowsFuture

          // Fetch values for RawRow which couldn't be fetched by SQL
          columnsWithFetchedValues <- Future.sequence(
            columns
              .zip(rawValues)
              .map({
                case (c: LinkColumn, array: JsonArray) if c.to.isInstanceOf[ConcatenateColumn] =>
                  // Fetch linked values of each linked row
                  fetchConcatValuesForLinkedRows(c.to.asInstanceOf[ConcatenateColumn], array)
                    .map(cellValue => (c, cellValue))

                case (c: AttachmentColumn, _) =>
                  // AttachmentColumns are fetched via AttachmentModel
                  retrieveCell(c, rowId)
                    .map(cell => (c, cell.value))

                case (c, value) =>
                  // All other column types were already fetched by RetrieveRowModel
                  Future.successful((c, value))
              })
          )

          // Generate values for GroupColumn && ConcatColumn
          columnsWithPostProcessedValues = columnsWithFetchedValues.map({
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
        } yield list ++ List(Row(table, rowId, rowLevelFlags, cellLevelFlags, columnsWithPostProcessedValues))
    }
  }

  def retrieveColumnValues(table: Table, columnId: ColumnId, langtagOpt: Option[String]): Future[Seq[String]] = {
    for {
      shortTextColumn <- retrieveColumn(table, columnId).flatMap({
        case shortTextColumn: ShortTextColumn => Future.successful(shortTextColumn)
        case column => Future.failed(WrongColumnKindException(column, classOf[ShortTextColumn]))
      })

      values <- retrieveRowModel.retrieveColumnValues(shortTextColumn, langtagOpt)
    } yield values
  }

  def retrieveTotalSize(table: Table): Future[Long] = {
    retrieveRowModel.size(table.id)
  }

  def retrieveCellHistory(table: Table,
                          columnId: ColumnId,
                          rowId: RowId,
                          langtagOpt: Option[String]): Future[SeqCellHistory] = {
    for {
      column <- retrieveColumn(table, columnId)
      cellHistorySeq <- retrieveHistoryModel.retrieve(table, column, rowId, langtagOpt)
    } yield cellHistorySeq
  }
}
