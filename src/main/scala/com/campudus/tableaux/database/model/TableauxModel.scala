package com.campudus.tableaux.database.model

import com.campudus.tableaux.InvalidJsonException
import com.campudus.tableaux.cache.CacheClient
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{CreateRowModel, RowModel, UpdateRowModel}
import org.vertx.scala.core.json._

import scala.concurrent.Future

object TableauxModel {
  type LinkId = Long
  type TableId = Long
  type ColumnId = Long
  type RowId = Long

  type Ordering = Long

  def apply(connection: DatabaseConnection): TableauxModel = {
    new TableauxModel(connection)
  }
}

/**
  * Needed because of {@code TableauxController#createCompleteTable} &
  * {@code TableauxController#retrieveCompleteTable}. Should only
  * be used by following delegate methods.
  */
sealed trait StructureDelegateModel extends DatabaseQuery {

  import TableauxModel._

  protected val connection: DatabaseConnection

  private lazy val structureModel = StructureModel(connection)

  def createTable(name: String, hidden: Boolean): Future[Table] = {
    structureModel.tableStruc.create(name, hidden, None)
  }

  def retrieveTable(tableId: TableId): Future[Table] = {
    structureModel.tableStruc.retrieve(tableId)
  }

  def createColumns(table: Table, columns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = {
    for {
      result <- structureModel.columnStruc.createColumns(table, columns)
    } yield result
  }

  def retrieveColumn(table: Table, columnId: ColumnId): Future[ColumnType[_]] = for {
    column <- structureModel.columnStruc.retrieve(table, columnId)
  } yield column

  def retrieveColumns(table: Table): Future[Seq[ColumnType[_]]] = for {
    columns <- structureModel.columnStruc.retrieveAll(table)
  } yield columns

  def checkValueTypeForColumn[A](column: ColumnType[_], value: A): Future[Unit] = {
    val checked = column.checkValidValue(value)
    checked.map(err => Future.failed(InvalidJsonException("malformed value provided", err))).getOrElse(Future.successful(()))
  }

  def retrieveDependencies(tableId: TableId): Future[Seq[(TableId, ColumnId)]] = {
    structureModel.columnStruc.retrieveDependencies(tableId)
  }
}

class TableauxModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery with StructureDelegateModel {

  import TableauxModel._

  val rowModel = new RowModel(connection)
  val createRowModel = new CreateRowModel(connection)
  val updateRowModel = new UpdateRowModel(connection)

  val attachmentModel = AttachmentModel(connection)

  def deleteRow(table: Table, rowId: RowId): Future[EmptyObject] = {
    for {
      columns <- retrieveColumns(table)
      _ <- rowModel.delete(table.id, rowId)

      // invalidate concat cell
      _ <- CacheClient(this.connection.vertx).invalidateCellValue(table.id, 0, rowId)
      // invalidate other cells
      _ <- Future.sequence(columns.map(column => CacheClient(this.connection.vertx).invalidateCellValue(table.id, column.id, rowId)))
    } yield EmptyObject()
  }

  def createRow(table: Table): Future[Row] = for {
    rowId <- createRowModel.createRow(table.id, Seq.empty)
    row <- retrieveRow(table, rowId)
  } yield row

  def createRows(table: Table, rows: Seq[Seq[(ColumnId, Any)]]): Future[RowSeq] = for {
    columns <- retrieveColumns(table)
    rows <- rows.foldLeft(Future.successful(Vector[Row]())) {
      (futureRows, row) =>
        // replace ColumnId with ColumnType
        val columnValuePairs = row.map { case (columnId, value) => (columns.find(_.id == columnId).get, value) }

        futureRows.flatMap { rows =>
          for {
            rowId <- createRowModel.createRow(table.id, columnValuePairs)
            newRow <- retrieveRow(table, columns, rowId)
          } yield {
            rows ++ Seq(newRow)
          }
        }
    }
  } yield RowSeq(rows)

  def updateCellValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- checkValueTypeForColumn(column, value)

      _ <- updateRowModel.updateRow(column.table, rowId, Seq((column, value)))

      _ <- invalidateCellAndDependentColumns(column, rowId)

      updatedCell <- retrieveCell(column, rowId)
    } yield updatedCell
  }

  def replaceCellValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- checkValueTypeForColumn(column, value)

      _ <- updateRowModel.clearRow(table, rowId, Seq(column))
      _ <- updateRowModel.updateRow(table, rowId, Seq((column, value)))

      _ <- invalidateCellAndDependentColumns(column, rowId)

      replacedCell <- retrieveCell(column, rowId)
    } yield replacedCell
  }

  def clearCellValue(table: Table, columnId: ColumnId, rowId: RowId): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)

      _ <- updateRowModel.clearRow(table, rowId, Seq(column))

      _ <- invalidateCellAndDependentColumns(column, rowId)

      replacedCell <- retrieveCell(column, rowId)
    } yield replacedCell
  }

  val CACHING = true

  private def invalidateCellAndDependentColumns(column: ColumnType[_], rowId: RowId): Future[Unit] = {
    if (!CACHING) {
      return Future.successful(())
    }

    for {
    // invalidate the cell itself
      _ <- CacheClient(this.connection.vertx).invalidateCellValue(column.table.id, column.id, rowId)

      // invalidate the concat cell if possible
      _ <- column.identifier match {
        case true => CacheClient(this.connection.vertx).invalidateCellValue(column.table.id, 0, rowId)
        case false => Future.successful(())
      }

      dependentColumns <- retrieveDependencies(column.table.id)

      _ <- Future.sequence(dependentColumns.map({
        case (tableId, columnId) =>
          // TODO perhaps we gain some performance if we only invalidate cells
          // TODO but therefore we would need to know the depending rows

          // invalidate depending columns
          CacheClient(this.connection.vertx).invalidateColumn(tableId, columnId)
            // and their concat column
            // TODO should be only done if depending column is an identifier column
            .zip(CacheClient(this.connection.vertx).invalidateColumn(tableId, 0))
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
      case c: ConcatColumn =>
        c.columns.+:(c)
      case _ =>
        Seq(column)
    }

    for {
      valueCache <- CACHING match {
        case true => CacheClient(this.connection.vertx).retrieveCellValue(column.table.id, column.id, rowId)
        case false => Future.successful(None)
      }

      value <- valueCache match {
        case Some(obj) =>
          // Cache hit
          Future.successful(obj)
        case None => {
          // Cache miss
          for {
            rawRow <- rowModel.retrieve(column.table.id, rowId, columns)

            rowSeq <- mapRawRows(column.table, columns, Seq(rawRow))

            // Because we only want a cell's value other
            // potential rows and columns can be ignored.
            value = rowSeq.head.values.head

            _ <- CACHING match {
              case true => CacheClient(this.connection.vertx).setCellValue(column.table.id, column.id, rowId, value)
              case false => Future.successful(())
            }
          } yield value
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
      rawRow <- rowModel.retrieve(table.id, rowId, columns)
      rowSeq <- mapRawRows(table, columns, Seq(rawRow))
    } yield rowSeq.head
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
        case c: ConcatColumn =>
          c.columns.+:(c)
        case _ =>
          Seq(column)
      }

      rowsSeq <- retrieveRows(table, columns, pagination)
    } yield {
      rowsSeq.copy(rows = rowsSeq.rows.map({
        row =>
          row.copy(values = row.values.take(1))
      }))
    }
  }

  private def retrieveRows(table: Table, columns: Seq[ColumnType[_]], pagination: Pagination): Future[RowSeq] = {
    for {
      totalSize <- rowModel.size(table.id)
      rawRows <- rowModel.retrieveAll(table.id, columns, pagination)
      rowSeq <- mapRawRows(table, columns, rawRows)
    } yield RowSeq(rowSeq, Page(pagination, Some(totalSize)))
  }

  def duplicateRow(table: Table, rowId: RowId): Future[Row] = {
    for {
      columns <- retrieveColumns(table).map(_.filter({
        // ConcatColumn can't be duplicated
        case _: ConcatColumn => false
        case _ => true
      }))

      // Retrieve row without ConcatColumn
      row <- retrieveRow(table, columns, rowId)
      rowValues = row.values

      duplicatedRowId <- createRowModel.createRow(table.id, columns.zip(rowValues))

      // Retrieve duplicated row with all columns
      duplicatedRow <- retrieveRow(table, duplicatedRowId)
    } yield duplicatedRow
  }

  private def mapRawRows(table: Table, columns: Seq[ColumnType[_]], rawRows: Seq[(RowId, Seq[Any])]): Future[Seq[Row]] = {

    def mapNullValueForLinkToConcat(concatColumn: ConcatColumn, array: JsonArray): Future[List[JsonObject]] = {
      import scala.collection.JavaConverters._

      // Iterate over each linked row and
      // replace json's value with ConcatColumn value
      array.asScala.foldLeft(Future.successful(List.empty[JsonObject])) {
        case (futureList, obj: JsonObject) =>
          futureList.flatMap({
            case list =>
              // ConcatColumn's value is always a
              // json array with the linked row ids
              val rowId = obj.getLong("id").longValue()
              retrieveCell(concatColumn, rowId)
                .map(cell => Json.obj("id" -> rowId, "value" -> cell.value))
                .map(json => list ++ List(json))
          })
      }
    }

    // TODO potential performance problem: foreach row every attachment column is mapped and attachments will be fetched!
    val mergedRowsFuture = rawRows.foldLeft(Future.successful(List.empty[(RowId, Seq[Any])])) {
      case (futureList, (rowId, rawValues)) =>
        futureList.flatMap({
          case list =>
            val mappedRow = columns.zip(rawValues).map({
              case (c: ConcatColumn, value) if c.columns.exists(col => col.kind == LinkType && col.asInstanceOf[LinkColumn[_]].to.isInstanceOf[ConcatColumn]) =>
                import scala.collection.JavaConverters._

                // Because of the guard we only handle ConcatColumns
                // with LinkColumns to another ConcatColumns

                // Zip concatenated columns with there values
                val concats = c.columns
                // value is a Java List because of RowModel.mapResultRow
                val values = value.asInstanceOf[java.util.List[Object]].asScala.toList

                assert(concats.size == values.size)

                val zippedColumnsValues = concats.zip(values)

                // Now we iterate over the zipped sequence and
                // check for LinkColumns which point to ConcatColumns
                val mappedColumnValues = zippedColumnsValues map {
                  case (column: LinkColumn[_], array: JsonArray) if column.to.isInstanceOf[ConcatColumn] =>
                    // Iterate over each linked row and
                    // replace json's value with ConcatColumn value
                    mapNullValueForLinkToConcat(column.to.asInstanceOf[ConcatColumn], array)

                  case (column, v) => Future.successful(v)
                }

                Future.sequence(mappedColumnValues)

              case (c: LinkColumn[_], array: JsonArray) if c.to.isInstanceOf[ConcatColumn] =>
                // Iterate over each linked row and
                // replace json's value with ConcatColumn value
                mapNullValueForLinkToConcat(c.to.asInstanceOf[ConcatColumn], array)

              case (c: AttachmentColumn, _) =>
                attachmentModel.retrieveAll(c.table.id, c.id, rowId)
              case (_, value) =>
                Future(value)
            })

            Future.sequence(mappedRow)
              .map(mappedRow => list ++ List((rowId, mappedRow)))
        })
    }

    val rows = mergedRowsFuture.map({
      mergedRows => mergedRows.map({
        case (rowId, values) => Row(table, rowId, values)
      })
    })

    rows
  }
}