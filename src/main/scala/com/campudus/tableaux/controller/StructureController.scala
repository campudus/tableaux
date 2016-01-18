package com.campudus.tableaux.controller

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.TableauxConfig
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel._

import scala.concurrent.Future

object StructureController {
  def apply(config: TableauxConfig, repository: StructureModel): StructureController = {
    new StructureController(config, repository)
  }
}

class StructureController(override val config: TableauxConfig, override protected val repository: StructureModel) extends Controller[StructureModel] {

  val tableStruc = repository.tableStruc
  val columnStruc = repository.columnStruc

  def retrieveTable(tableId: TableId): Future[Table] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveTable $tableId")

    tableStruc.retrieve(tableId)
  }

  def retrieveTables(): Future[TableSeq] = {
    logger.info(s"retrieveTables")

    tableStruc.retrieveAll().map(TableSeq)
  }

  def createColumns(tableId: TableId, columns: Seq[CreateColumn]): Future[ColumnSeq] = {
    checkArguments(greaterZero(tableId), nonEmpty(columns, "columns"))
    logger.info(s"createColumns $tableId columns $columns")

    for {
      table <- retrieveTable(tableId)
      columns <- columnStruc.createColumns(table, columns)
    } yield {
      logger.info(s"$columns")
      ColumnSeq(columns)
    }
  }

  def retrieveColumn(tableId: TableId, columnId: ColumnId): Future[ColumnType[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"retrieveColumn $tableId $columnId")

    for {
      column <- columnStruc.retrieve(tableId, columnId)
    } yield column
  }

  def retrieveColumns(tableId: TableId): Future[DomainObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"retrieveColumns $tableId")

    for {
      columns <- columnStruc.retrieveAll(tableId)
    } yield ColumnSeq(columns)
  }

  def createTable(tableName: String, hidden: Option[Boolean]): Future[Table] = {
    checkArguments(notNull(tableName, "tableName"))
    logger.info(s"createTable $tableName")

    tableStruc.create(tableName, hidden.getOrElse(false))
  }

  def deleteTable(tableId: TableId): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId))
    logger.info(s"deleteTable $tableId")

    for {
      columns <- columnStruc.retrieveAll(tableId)
      // only delete special column before deleting table;
      // e.g. link column are based on simple columns
      _ <- Future.sequence(columns.map({
        case column@(_: LinkColumn[_]) => columnStruc.delete(tableId, column.id)
        case column@(_: AttachmentColumn) => columnStruc.delete(tableId, column.id)
        case _ => Future(())
      }))
      _ <- tableStruc.delete(tableId)
    } yield EmptyObject()
  }

  def deleteColumn(tableId: TableId, columnId: ColumnId): Future[EmptyObject] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"deleteColumn $tableId $columnId")

    for {
      _ <- columnStruc.delete(tableId, columnId)
    } yield EmptyObject()
  }

  def changeTable(tableId: TableId, tableName: String): Future[Table] = {
    checkArguments(greaterZero(tableId), notNull(tableName, "tableName"))
    logger.info(s"changeTable $tableId $tableName")

    for {
      _ <- tableStruc.change(tableId, tableName)
      table <- tableStruc.retrieve(tableId)
    } yield table
  }

  def changeColumn(tableId: TableId, columnId: ColumnId, columnName: Option[String], ordering: Option[Ordering], kind: Option[TableauxDbType], identifier: Option[Boolean]): Future[ColumnType[_]] = {
    checkArguments(greaterZero(tableId), greaterZero(columnId))
    logger.info(s"changeColumn $tableId $columnId $columnName $ordering $kind")

    for {
      _ <- columnStruc.change(tableId, columnId, columnName, ordering, kind, identifier)
      column <- retrieveColumn(tableId, columnId)
    } yield column
  }
}
