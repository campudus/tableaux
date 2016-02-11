package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.tableaux.{CellModel, RowModel}
import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.{ArgumentChecker, InvalidJsonException}
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.Try

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
    structureModel.tableStruc.create(name, hidden)
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
}

class TableauxModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery with StructureDelegateModel {

  import TableauxModel._

  val cellModel = new CellModel(connection)
  val rowModel = new RowModel(connection)

  val attachmentModel = AttachmentModel(connection)

  def deleteRow(table: Table, rowId: RowId): Future[EmptyObject] = for {
    _ <- rowModel.delete(table.id, rowId)
  } yield EmptyObject()

  def createRow(table: Table): Future[Row] = for {
    rowId <- rowModel.createEmpty(table.id)
    row <- retrieveRow(table, rowId)
  } yield row

  private def createRow(table: Table, values: Seq[(ColumnType[_], Any)]): Future[Row] = for {
    rowId <- rowModel.createRow(table.id, values)
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
            rowId <- rowModel.createRow(table.id, columnValuePairs)
            newRow <- retrieveRow(table, columns,rowId)
          } yield {
            rows ++ Seq(newRow)
          }
        }
    }
  } yield RowSeq(rows)

  def updateValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(table, columnId)
      _ <- checkValueTypeForColumn(column, value)
      cell <- column match {
        case column: AttachmentColumn => updateAttachment(column, rowId, value)
        case _ => insertValue(column, rowId, value)
      }
    } yield cell
  }

  def insertValue[A](table: Table, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = for {
    column <- retrieveColumn(table, columnId)
    _ <- checkValueTypeForColumn(column, value)
    cell <- insertValue[A](column, rowId, value)
  } yield cell

  private def insertValue[A](column: ColumnType[_], rowId: RowId, value: A): Future[Cell[_]] = for {
    cell <- column match {
      case column: AttachmentColumn => insertAttachment[A](column, rowId, value)
      case column: LinkColumn[_] => handleLinkValues[A](column, rowId, value)
      case column: MultiLanguageColumn[_] => insertMultiLanguageValues(column, rowId, value.asInstanceOf[JsonObject])
      case _ => insertSimpleValue(column, rowId, value)
    }
  } yield cell

  private def insertAttachment[A](column: AttachmentColumn, rowId: RowId, value: A): Future[Cell[Any]] = {
    // add attachment
    handleAttachment(column, rowId, value, attachmentModel.add)
  }

  private def updateAttachment[A](column: AttachmentColumn, rowId: RowId, value: A): Future[Cell[Any]] = {
    // update attachment order
    handleAttachment(column, rowId, value, attachmentModel.update)
  }

  private def handleAttachment[A](column: AttachmentColumn, rowId: RowId, value: A, fn: Attachment => Future[AttachmentFile]): Future[Cell[Any]] = {
    import ArgumentChecker._

    val v = Try(Left(value.asInstanceOf[JsonObject])).orElse(Try(Right(value.asInstanceOf[JsonArray]))).get

    v match {
      case Left(obj) =>
        // single attachment
        val uuid = notNull(obj.getString("uuid"), "uuid").map(UUID.fromString).get
        val ordering: Option[Ordering] = {
          if (obj.containsField("ordering")) {
            Option(obj.getLong("ordering"))
          } else {
            None
          }
        }

        for {
          file <- fn(Attachment(column.table.id, column.id, rowId, uuid, ordering))
          cell <- retrieveCell(column, rowId)
        } yield cell

      case Right(arr) =>
        // multiple attachments
        val attachments = scala.collection.mutable.ListBuffer.empty[Attachment]

        for (i <- 0 until arr.size()) {
          val obj = arr.get[JsonObject](i)

          val uuid = notNull(obj.getString("uuid"), "uuid").map(UUID.fromString).get
          val ordering: Option[Ordering] = {
            if (obj.containsField("ordering")) {
              Option(obj.getLong("ordering"))
            } else {
              None
            }
          }

          attachments += Attachment(column.table.id, column.id, rowId, uuid, ordering)
        }

        for {
          _ <- attachmentModel.replace(column.table.id, column.id, rowId, attachments)
          cell <- retrieveCell(column.table, column.id, rowId)
        } yield cell
    }
  }

  private def insertMultiLanguageValues[A <: JsonObject](column: MultiLanguageColumn[_], rowId: RowId, value: A): Future[Cell[Any]] = {
    for {
      _ <- cellModel.updateTranslations(column.table, column, rowId, JsonUtils.toTupleSeq(value))
      // We need to retrieve cell again
      // because we could have only changed on language
      cell <- retrieveCell(column, rowId)
    } yield cell
  }

  private def insertSimpleValue[A](column: ColumnType[A], rowId: RowId, value: A): Future[Cell[Any]] = for {
    _ <- cellModel.update(column.table, column, rowId, value)
    cell <- retrieveCell(column, rowId)
  } yield cell

  private def handleLinkValues[A](column: LinkColumn[_], rowId: RowId, value: A): Future[Cell[Any]] = {
    Try(value.asInstanceOf[JsonObject]).flatMap { v =>
      import ArgumentChecker._

      import collection.JavaConverters._

      for {
        toOrValues <- Try(Left(checked(hasLong("to", v))))
          .orElse(Try(Right(checked(hasArray("values", v)).asScala.map(_.asInstanceOf[Number].longValue()).toSeq)))
      } yield {
        toOrValues match {
          case Left(toId) => addLinkValue(column, rowId, toId)
          case Right(toIds) => insertLinkValues(column, rowId, toIds)
        }
      }
    } getOrElse {
      Future.failed(InvalidJsonException(s"A link column expects a JSON object with to values, but got $value", "link-value"))
    }
  }

  //TODO we should remove its in favor of addLinkValue because of raise conditions
  @Deprecated
  private def insertLinkValues(linkColumn: LinkColumn[_], fromId: RowId, toIds: Seq[RowId]): Future[Cell[Any]] = for {
    _ <- cellModel.putLinks(linkColumn.table, linkColumn, fromId, toIds)
    cell <- retrieveCell(linkColumn, fromId)
  } yield cell

  private def addLinkValue(linkColumn: LinkColumn[_], fromId: RowId, toId: RowId): Future[Cell[Any]] = {
    for {
      _ <- cellModel.updateLink(linkColumn.table, linkColumn, fromId, toId)
      cell <- retrieveCell(linkColumn, fromId)
    } yield cell
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
      rawRow <- rowModel.retrieve(column.table.id, rowId, columns)
      rowSeq <- mapRawRows(column.table, columns, Seq(rawRow))

      // Because we only want a cell's value other
      // potential rows and columns can be ignored.
      value = rowSeq.head.values.head
    } yield Cell(column.asInstanceOf[ColumnType[Any]], rowId, value)
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
      rows <- retrieveRows(table, Seq(column), pagination)
    } yield rows
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
      columns <- retrieveColumns(table).map({
        _.filter({
          // ConcatColumn can't be duplicated
          case _: ConcatColumn => false
          case _ => true
        })
      })

      // Retrieve row without ConcatColumn
      row <- retrieveRow(table, columns, rowId)
      rowValues = row.values

      duplicatedRowId <- rowModel.createRow(table.id, columns.zip(rowValues))

      // Retrieve duplicated row with all columns
      duplicatedRow <- retrieveRow(table, duplicatedRowId)
    } yield duplicatedRow
  }

  private def mapRawRows(table: Table, columns: Seq[ColumnType[_]], rawRows: Seq[(RowId, Seq[AnyRef])]): Future[Seq[Row]] = {

    def mapNullValueForLinkToConcat(concatColumn: ConcatColumn, array: JsonArray): Future[List[Any]] = {
      import scala.collection.JavaConversions._

      // Iterate over each linked row and
      // replace json's value with ConcatColumn value
      Future.sequence(array.toList.map({
        case obj: JsonObject =>
          // ConcatColumn's value is always a
          // json array with the linked row ids
          val rowId = obj.getLong("id").longValue()
          retrieveCell(concatColumn, rowId).map(cell => Json.obj("id" -> rowId, "value" -> cell.value))
      }))
    }

    // TODO potential performance problem: foreach row every attachment column is mapped and attachments will be fetched!
    val mergedRows = rawRows map {
      case (rowId, rawValues) =>

        val mergedValues = Future.sequence((columns, rawValues).zipped map {
          case (c: ConcatColumn, value) if c.columns.exists(col => col.kind == LinkType && col.asInstanceOf[LinkColumn[_]].to.isInstanceOf[ConcatColumn]) =>
            import scala.collection.JavaConversions._
            import scala.collection.JavaConverters._

            // Because of the guard we only handle ConcatColumns
            // with LinkColumns to another ConcatColumns

            // Zip concatenated columns with there values
            val concats = c.columns
            // value is a Java List because of RowModel.mapResultRow
            val values = value.asInstanceOf[java.util.List[Object]].toList

            assert(concats.size == values.size)

            val zippedColumnsValues = concats.zip(values)

            // Now we iterate over the zipped sequence and
            // check for LinkColumns which point to ConcatColumns
            val mappedColumnValues = zippedColumnsValues map {
              case (column: LinkColumn[_], array: JsonArray) if column.to.isInstanceOf[ConcatColumn] =>
                // Iterate over each linked row and
                // replace json's value with ConcatColumn value
                mapNullValueForLinkToConcat(column.to.asInstanceOf[ConcatColumn], array).map(_.asJava)

              case (column, v) => Future.successful(v)
            }

            // We need a Java collection so it
            // can be serialized to JsonArray by vertx
            Future.sequence(mappedColumnValues).map(_.asJava)

          case (c: LinkColumn[_], array: JsonArray) if c.to.isInstanceOf[ConcatColumn] =>
            import scala.collection.JavaConverters._

            // Iterate over each linked row and
            // replace json's value with ConcatColumn value

            mapNullValueForLinkToConcat(c.to.asInstanceOf[ConcatColumn], array).map(_.asJava)

          case (c: AttachmentColumn, _) => attachmentModel.retrieveAll(c.table.id, c.id, rowId)
          case (_, value) => Future(value)
        })

        (rowId, mergedValues)
    }

    val rows = mergedRows map {
      case (rowId, mergedValues) => mergedValues.map(Row(table, rowId, _))
    }

    Future.sequence(rows)
  }
}