package com.campudus.tableaux.database

import com.campudus.tableaux.database.structure._
import com.campudus.tableaux.helper.StandardVerticle
import org.vertx.scala.core.json._
import org.vertx.scala.platform.Verticle

import scala.concurrent.Future

object Tableaux {
  type IdType = Long
  type Ordering = Long
  type LinkConnection = (IdType, IdType, IdType)
}

class Tableaux(val verticle: Verticle, val database: DatabaseConnection) extends DatabaseAccess with StandardVerticle {
  import Tableaux._

  val systemStruc = new SystemStructure(database)
  val tableStruc = new TableStructure(database)
  val columnStruc = new ColumnStructure(database)
  val cellStruc = new CellStructure(database)
  val rowStruc = new RowStructure(database)

  def resetDB(): Future[EmptyObject] = for {
    _ <- systemStruc.deinstall()
    _ <- systemStruc.setup()
  } yield EmptyObject()

  def createTable(name: String): Future[Table] = for {
    id <- tableStruc.create(name)
  } yield Table(id, name)

  def createCompleteTable(name: String, columnsNameAndType: Seq[CreateColumn], rowsValues: Seq[Seq[_]]): Future[CompleteTable] = for {
    table <- createTable(name)
    columnIds <- addColumns(table.id, columnsNameAndType) map { colSeq => colSeq.columns map { col => col.id } }
    rowsWithColumnIdAndValue <- Future.successful {
      if (rowsValues.isEmpty) {
        Seq()
      } else {
        rowsValues map { columnIds.zip(_) }
      }
    }
    _ <- addFullRows(table.id, rowsWithColumnIdAndValue)
    completeTable <- getCompleteTable(table.id)
  } yield completeTable

  def deleteTable(id: IdType): Future[EmptyObject] = for {
    _ <- tableStruc.delete(id)
  } yield EmptyObject()

  def deleteRow(tableId: IdType, rowId: IdType): Future[EmptyObject] = for {
    _ <- rowStruc.delete(tableId, rowId)
  } yield EmptyObject()

  def addColumns(tableId: IdType, columns: Seq[CreateColumn]): Future[ColumnSeq] = for {
    cols <- serialiseFutures(columns) {
      case CreateColumn(name, LinkType, optOrd, Some((toTable, toColumn, fromColumn))) =>
        addLinkColumn(tableId, name, fromColumn, toTable, toColumn, optOrd)
      case CreateColumn(name, dbType, optOrd, optLink) =>
        addValueColumn(tableId, name, dbType, optOrd)
    }
  } yield ColumnSeq(cols)

  def addValueColumn(tableId: IdType, name: String, columnType: TableauxDbType, ordering: Option[Ordering]): Future[ColumnValue[_]] = for {
    table <- getTable(tableId)
    (id, ordering) <- columnStruc.insert(table.id, columnType, name, ordering)
  } yield Mapper.getApply(columnType).apply(table, id, name, ordering)

  def addLinkColumn(tableId: IdType, name: String, fromColumn: IdType, toTable: IdType, toColumn: IdType, ordering: Option[Ordering]): Future[LinkColumnType[_]] = for {
    table <- getTable(tableId)
    toCol <- getColumn(toTable, toColumn).asInstanceOf[Future[ColumnValue[_]]]
    (id, ordering) <- columnStruc.insertLink(tableId, name, fromColumn, toCol.table.id, toCol.id, ordering)
  } yield LinkColumn(table, id, toCol, name, ordering)

  def removeColumn(tableId: IdType, columnId: IdType): Future[EmptyObject] = for {
    _ <- columnStruc.delete(tableId, columnId)
  } yield EmptyObject()

  def addRow(tableId: IdType): Future[RowIdentifier] = for {
    table <- getTable(tableId)
    id <- rowStruc.create(tableId)
  } yield RowIdentifier(table, id)

  def addFullRows(tableId: IdType, values: Seq[Seq[(IdType, _)]]): Future[RowSeq] = for {
    table <- getTable(tableId)
    ids <- serialiseFutures(values)(rowStruc.createFull(table.id, _))
    row <- serialiseFutures(ids)(getRow(table.id, _))
  } yield RowSeq(row)

  private def serialiseFutures[A, B](seq: Seq[A])(fn: A => Future[B]): Future[Seq[B]] = {
    seq.foldLeft(Future(Seq.empty[B])) {
      (lastFuture, next) => lastFuture flatMap { preResults => fn(next) map { preResults :+ _ } }
    }
  }

  def insertValue[A](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[_, _]] = for {
    column <- getColumn(tableId, columnId)
    cell <- column match {
      case _: LinkColumnType[_] =>
        import scala.collection.JavaConverters._
        val valueList = value.asInstanceOf[JsonArray].asScala.toList.asInstanceOf[List[Number]]
        val valueFromList = (valueList.head.longValue(), valueList(1).longValue())
        insertLinkValue(tableId, columnId, rowId, valueFromList)
      case _ => insertNormalValue(tableId, columnId, rowId, value)
    }
  } yield cell

  def insertNormalValue[A, B <: ColumnType[A]](tableId: IdType, columnId: IdType, rowId: IdType, value: A): Future[Cell[A, B]] = for {
    column <- getColumn(tableId, columnId)
    _ <- cellStruc.update(tableId, columnId, rowId, value)
  } yield Cell(column.asInstanceOf[B], rowId, value)

  def insertLinkValue(tableId: IdType, columnId: IdType, rowId: IdType, value: (IdType, IdType)): Future[Cell[Link[Any], LinkColumnType[Any]]] = for {
    linkColumn <- getColumn(tableId, columnId).asInstanceOf[Future[LinkColumnType[Any]]]
    _ <- cellStruc.updateLink(linkColumn.table.id, linkColumn.id, value)
    v <- cellStruc.getLinkValues(linkColumn.table.id, linkColumn.id, rowId, linkColumn.to.table.id, linkColumn.to.id)
  } yield Cell(linkColumn, rowId, Link(List((value._2, v))))

  def getAllTables(): Future[TableSeq] = for {
    seqOfTableInformation <- tableStruc.getAll
  } yield {
      TableSeq(seqOfTableInformation map { case (id, name) => Table(id, name) })
    }

  def getTable(tableId: IdType): Future[Table] = for {
    (id, name) <- tableStruc.get(tableId)
  } yield Table(id, name)

  def getRow(tableId: IdType, rowId: IdType): Future[Row] = for {
    table <- getTable(tableId)
    (id, seqOfValues) <- rowStruc.get(tableId, rowId)
  } yield Row(table, id, seqOfValues)

  def getCell(tableId: IdType, columnId: IdType, rowId: IdType): Future[Cell[_, _]] = for {
    column <- getColumn(tableId, columnId)
    value <- column match {
      case c: LinkColumn[_] => cellStruc.getLinkValues(c.table.id, c.id, rowId, c.to.table.id, c.to.id)
      case _ => cellStruc.getValue(tableId, columnId, rowId)
    }
  } yield Cell(column.asInstanceOf[ColumnType[Any]], rowId, value)

  def getColumn(tableId: IdType, columnId: IdType): Future[ColumnType[_]] = for {
    table <- getTable(tableId)
    (columnId, columnName, columnKind, ordering) <- columnStruc.get(table.id, columnId)
    column <- columnKind match {
      case LinkType => getLinkColumn(table, columnId, columnName, ordering)
      case kind => Future.successful(getValueColumn(table, columnId, columnName, kind, ordering))
    }
  } yield column

  private def getValueColumn(table: Table, columnId: IdType, columnName: String, columnKind: TableauxDbType, ordering: Ordering): ColumnValue[_] = {
    Mapper.getApply(columnKind).apply(table, columnId, columnName, ordering)
  }

  private def getLinkColumn(table: Table, columnId: IdType, columnName: String, ordering: Ordering): Future[LinkColumnType[_]] = for {
    (tableId, toColumnId) <- columnStruc.getToColumn(table.id, columnId)
    toCol <- getColumn(tableId, toColumnId).asInstanceOf[Future[ColumnValue[_]]]
  } yield LinkColumn(table, columnId, toCol, columnName, ordering)

  def getCompleteTable(tableId: IdType): Future[CompleteTable] = for {
    table <- getTable(tableId)
    colList <- getAllColumns(table)
    rowList <- getAllRows(table)
  } yield CompleteTable(table, colList, rowList)

  private def getAllColumns(table: Table): Future[Seq[ColumnType[_]]] = for {
    allColumns <- columnStruc.getAll(table.id)
  } yield allColumns map {
      case (columnId, columnName, columnKind, ordering) =>
        Mapper.getApply(columnKind).apply(table, columnId, columnName, ordering)
    }

  private def getAllRows(table: Table): Future[RowSeq] = for {
    allRows <- rowStruc.getAll(table.id)
  } yield RowSeq(allRows map { case (rowId, values) => Row(table, rowId, values) })

  def changeTableName(tableId: IdType, tableName: String): Future[Table] = for {
    _ <- tableStruc.changeName(tableId, tableName)
  } yield Table(tableId, tableName)

  def changeColumn(tableId: IdType, columnId: IdType, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType]): Future[ColumnType[_]] = for {
    _ <- columnStruc.change(tableId, columnId, columnName, ordering, kind)
    column <- getColumn(tableId, columnId)
  } yield column
}