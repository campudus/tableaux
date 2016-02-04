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

  def createColumns(tableId: TableId, columns: Seq[CreateColumn]): Future[Seq[ColumnType[_]]] = {
    for {
      table <- retrieveTable(tableId)
      result <- structureModel.columnStruc.createColumns(table, columns)
    } yield result
  }

  def retrieveTable(tableId: TableId): Future[Table] = {
    structureModel.tableStruc.retrieve(tableId)
  }

  def retrieveColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = for {
    table <- retrieveTable(tableId)
    column <- structureModel.columnStruc.retrieve(table, columnId)
  } yield column

  def retrieveColumns(tableId: TableId): Future[Seq[ColumnType[_]]] = for {
    table <- retrieveTable(tableId)
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

  def deleteRow(tableId: TableId, rowId: RowId): Future[EmptyObject] = for {
    _ <- rowModel.delete(tableId, rowId)
  } yield EmptyObject()

  def createRow(tableId: TableId): Future[Row] = for {
    table <- retrieveTable(tableId)
    id <- rowModel.createEmpty(tableId)
  } yield Row(table, id, Seq.empty)

  def createRows(tableId: TableId, rows: Seq[Seq[(ColumnId, Any)]]): Future[RowSeq] = for {
    table <- retrieveTable(tableId)
    columns <- retrieveColumns(table.id)
    rows <- rows.foldLeft(Future.successful(Vector[Row]())) {
      (futureRows, row) =>
        // replace ColumnId with ColumnType
        val columnValuePairs = row.map { case (columnId, value) => (columns.find(_.id == columnId).get, value) }

        futureRows.flatMap { rows =>
          for {
            rowId <- rowModel.createRow(table.id, columnValuePairs)
            rawRow <- rowModel.retrieve(table.id, rowId, columns)
            rowSeq <- mapRawRows(table, columns, Seq(rawRow))
          } yield {
            logger.info(s"created row $rowId")
            rows ++ rowSeq
          }
        }
    }
  } yield RowSeq(rows)

  def updateValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = {
    for {
      column <- retrieveColumn(tableId, columnId)
      _ <- checkValueTypeForColumn(column, value)
      cell <- column match {
        case column: AttachmentColumn => updateAttachment(column, rowId, value)
        case _ => insertValue(column, rowId, value)
      }
    } yield cell
  }

  def insertValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = for {
    column <- retrieveColumn(tableId, columnId)
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
          column <- retrieveColumn(column.table.id, column.id)
          file <- fn(Attachment(column.table.id, column.id, rowId, uuid, ordering))
        } yield Cell(column.asInstanceOf[AttachmentColumn], rowId, file)

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
          column <- retrieveColumn(column.table.id, column.id)
          file <- attachmentModel.replace(column.table.id, column.id, rowId, attachments)
          cell <- retrieveCell(column.table.id, column.id, rowId)
        } yield cell
    }
  }

  private def insertMultiLanguageValues[A <: JsonObject](column: MultiLanguageColumn[_], rowId: RowId, value: A): Future[Cell[Any]] = {
    for {
      _ <- cellModel.updateTranslations(column.table, column, rowId, JsonUtils.toTupleSeq(value))
    } yield Cell(column, rowId, value)
  }

  private def insertSimpleValue[A, B <: ColumnType[A]](column: ColumnType[_], rowId: RowId, value: A): Future[Cell[A]] = for {
    _ <- cellModel.update(column.table, column, rowId, value)
  } yield Cell(column.asInstanceOf[B], rowId, value)

  private def handleLinkValues[A](column: LinkColumn[_], rowId: RowId, value: A): Future[Cell[Link[_]]] = {
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

  private def insertLinkValues(linkColumn: LinkColumn[_], fromId: RowId, toIds: Seq[RowId]): Future[Cell[Link[_]]] = for {
    _ <- cellModel.putLinks(linkColumn.table, linkColumn, fromId, toIds)
    v <- retrieveCell(linkColumn, fromId)
  } yield Cell(linkColumn, fromId, Link(linkColumn.to.id, v))

  private def addLinkValue(linkColumn: LinkColumn[_], fromId: RowId, toId: RowId): Future[Cell[Link[_]]] = {
    logger.info(s"in addLinkValue $linkColumn -> $fromId to $toId")
    for {
      _ <- cellModel.updateLink(linkColumn.table, linkColumn, fromId, toId)
      _ = logger.info(s"link updated!")
      v <- retrieveCell(linkColumn, fromId)
    } yield Cell(linkColumn, fromId, Link(toId, v))
  }

  def retrieveCell(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Cell[Any]] = {
    for {
      column <- retrieveColumn(tableId, columnId)
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

  def retrieveRow(tableId: TableId, rowId: RowId): Future[Row] = {
    for {
      table <- retrieveTable(tableId)
      columns <- retrieveColumns(table.id)
      rawRow <- rowModel.retrieve(tableId, rowId, columns)
      rowSeq <- mapRawRows(table, columns, Seq(rawRow))
    } yield rowSeq.head
  }

  def retrieveRows(table: Table, pagination: Pagination): Future[RowSeq] = {
    for {
      columns <- retrieveColumns(table.id)
      rows <- retrieveRows(table, columns, pagination)
    } yield rows
  }

  def retrieveRows(table: Table, columnId: ColumnId, pagination: Pagination): Future[RowSeq] = {
    for {
      column <- retrieveColumn(table.id, columnId)
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

  def duplicateRow(tableId: TableId, rowId: RowId): Future[Row] = {
    for {
      table <- retrieveTable(tableId)
      columns <- retrieveColumns(table.id)
      rowIdValues <- rowModel.retrieve(tableId, rowId, columns)
      rows <- mapRawRows(table, columns, Seq(rowIdValues))
      rowValues = rows.head.values
      duplicatedSimpleRowId <- rowModel.createRow(tableId, columns.zip(rowValues))
      (duplicatedRowId, duplicatedRowValues) <- rowModel.retrieve(tableId, duplicatedSimpleRowId, columns)
    } yield Row(table, duplicatedRowId, duplicatedRowValues)
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