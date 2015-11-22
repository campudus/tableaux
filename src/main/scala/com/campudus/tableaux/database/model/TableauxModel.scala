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

  def createTable(name: String): Future[Table] = {
    structureModel.tableStruc.create(name)
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

  def retrieveColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = {
    structureModel.columnStruc.retrieve(tableId, columnId)
  }

  def retrieveColumns(tableId: TableId): Future[Seq[ColumnType[_]]] = {
    structureModel.columnStruc.retrieveAll(tableId)
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
    ids <- Future.sequence(rows.map({
      row =>
        // replace ColumnId with ColumnType
        val mappedRow = row.map { case (columnId, value) => (columns.find(_.id == columnId).get, value) }

        val singleValues = mappedRow.filter {
          case (_: MultiLanguageColumn[_], _) => false
          case (_, _) => true
        }

        val multiValues = mappedRow.filter {
          case (_: MultiLanguageColumn[_], _) => true
          case (_, _) => false
        } map { case (column, value) =>
          (column, JsonUtils.toTupleSeq(value.asInstanceOf[JsonObject]))
        }

        for {
          rowId <- if (singleValues.isEmpty) {
            rowModel.createEmpty(table.id)
          } else {
            rowModel.createFull(table.id, singleValues)
          }

          _ <- if (multiValues.nonEmpty) {
            rowModel.createTranslations(table.id, rowId, multiValues)
          } else {
            Future.successful(())
          }
        } yield rowId
    }))
    rows <- Future.sequence(ids.map(retrieveRow(table.id, _)))
  } yield RowSeq(rows)

  def updateValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = for {
  // TODO column should be passed on to insert methods
    column <- retrieveColumn(tableId, columnId)
    cell <- column match {
      case attachmentColumn: AttachmentColumn => updateAttachment(tableId, columnId, rowId, value)
      case _ => insertValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertValue[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[_]] = for {
  // TODO column should be passed on to insert methods
    column <- retrieveColumn(tableId, columnId)
    cell <- column match {
      case column: AttachmentColumn => insertAttachment(tableId, columnId, rowId, value)
      case column: LinkColumn[_] => handleLinkValues(tableId, columnId, rowId, value)
      case column: MultiLanguageColumn[_] => insertMultiLanguageValues(tableId, columnId, rowId, value.asInstanceOf[JsonObject])
      case _ => insertSimpleValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertAttachment[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[Any]] = {
    // add attachment
    handleAttachment(tableId, columnId, rowId, value, attachmentModel.add)
  }

  def updateAttachment[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[Any]] = {
    // update attachment order
    handleAttachment(tableId, columnId, rowId, value, attachmentModel.update)
  }

  private def handleAttachment[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A, fn: Attachment => Future[AttachmentFile]): Future[Cell[Any]] = {
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
          column <- retrieveColumn(tableId, columnId)
          file <- fn(Attachment(tableId, columnId, rowId, uuid, ordering))
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

          attachments += Attachment(tableId, columnId, rowId, uuid, ordering)
        }

        for {
          column <- retrieveColumn(tableId, columnId)
          file <- attachmentModel.replace(tableId, columnId, rowId, attachments)
          cell <- retrieveCell(tableId, columnId, rowId)
        } yield cell
    }
  }

  private def insertMultiLanguageValues[A <: JsonObject](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[Any]] = {
    for {
      column <- retrieveColumn(tableId, columnId)
      _ <- cellModel.updateTranslations(column.table.id, column.id, rowId, JsonUtils.toTupleSeq(value))
    } yield Cell(column, rowId, value)
  }

  private def insertSimpleValue[A, B <: ColumnType[A]](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[A]] = for {
    column <- retrieveColumn(tableId, columnId)
    _ <- cellModel.update(tableId, columnId, rowId, value)
  } yield Cell(column.asInstanceOf[B], rowId, value)

  private def handleLinkValues[A](tableId: TableId, columnId: ColumnId, rowId: RowId, value: A): Future[Cell[Link[Any]]] = {
    Try(value.asInstanceOf[JsonObject]).flatMap { v =>
      import ArgumentChecker._

      import collection.JavaConverters._

      for {
        toIds <- Try(Left(checked(hasNumber("to", v)).longValue())).orElse(
          Try(Right(checked(hasArray("values", v)).asScala.map(_.asInstanceOf[Number].longValue()).toSeq)))
      } yield {
        toIds match {
          case Left(toId) => addLinkValue(tableId, columnId, rowId, toId)
          case Right(toIds) => insertLinkValues(tableId, columnId, rowId, toIds)
        }
      }
    } getOrElse {
      Future.failed(InvalidJsonException(s"A link column expects a JSON object with from and to values, but got $value", "link-value"))
    }
  }

  private def insertLinkValues(tableId: TableId, columnId: ColumnId, fromId: RowId, toIds: Seq[RowId]): Future[Cell[Link[Any]]] = for {
    linkColumn <- retrieveColumn(tableId, columnId).asInstanceOf[Future[LinkColumn[Any]]]
    _ <- cellModel.putLinks(linkColumn.table.id, linkColumn.id, fromId, toIds)
    v <- cellModel.getLinkValues(linkColumn, fromId)
  } yield Cell(linkColumn, fromId, Link(linkColumn.to.id, v))

  def addLinkValue(tableId: TableId, columnId: ColumnId, fromId: RowId, toId: RowId): Future[Cell[Link[Any]]] = for {
    linkColumn <- retrieveColumn(tableId, columnId).asInstanceOf[Future[LinkColumn[Any]]]
    _ <- cellModel.updateLink(linkColumn.table.id, linkColumn.id, fromId, toId)
    v <- cellModel.getLinkValues(linkColumn, fromId)
  } yield Cell(linkColumn, fromId, Link(toId, v))

  def retrieveCell(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[Cell[Any]] = {
    for {
      table <- retrieveTable(tableId)
      column <- retrieveColumn(tableId, columnId)
      rawRow <- rowModel.retrieve(tableId, rowId, Seq(column))
      rowSeq <- mapRawRows(table, Seq(column), Seq(rawRow))
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

  private def mapRawRows(table: Table, columns: Seq[ColumnType[_]], rawRows: Seq[(RowId, Seq[AnyRef])]): Future[Seq[Row]] = {
    // TODO performance problem: foreach row every attachment column is mapped and attachments will be fetched!
    val mergedRows = rawRows map {
      case (rowId, rawValues) =>

        val mergedValues = Future.sequence((columns, rawValues).zipped map {
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