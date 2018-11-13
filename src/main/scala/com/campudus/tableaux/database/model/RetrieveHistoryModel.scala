package com.campudus.tableaux.database.model

import java.util.UUID

import com.campudus.tableaux.UnknownServerException
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering, RowId, TableId}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future

//case class Attachment(tableId: TableId, columnId: ColumnId, rowId: RowId, uuid: UUID, ordering: Option[Ordering])
//
//case class AttachmentFile(file: ExtendedFile, ordering: Ordering) extends DomainObject {
//
//  override def getJson: JsonObject = Json.obj("ordering" -> ordering).mergeIn(file.getJson)
//}

object RetrieveHistoryModel {

  def apply(connection: DatabaseConnection): RetrieveHistoryModel = {
    new RetrieveHistoryModel(connection)
  }
}

class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
//  def retrieveCells(file: UUID): Future[Seq[(TableId, ColumnId, RowId)]] = {
//    val select = s"SELECT table_id, column_id, row_id FROM $table WHERE attachment_uuid = ?"
//
//    for {
//      result <- connection.query(select, Json.arr(file.toString))
//      cells = resultObjectToJsonArray(result).map(e => (e.get[TableId](0), e.get[ColumnId](1), e.get[RowId](2)))
//    } yield cells
//  }

  def retrieve(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[SeqCellHistory] = {
    val select =
      s"""
         |  SELECT
         |    revision,
         |    column_type,
         |    multilanguage,
         |    value,
         |    author,
         |    timestamp
         |  FROM
         |    user_table_history_2
         |  WHERE
         |    row_id = 1
         |    AND column_id = 1
         |    AND event = 'cell_changed'
         |  ORDER BY
         |    timestamp ASC
         """.stripMargin

    for {
      result <- connection.query(select)
    } yield {
//      val cellHistory = resultObjectToJsonArray(result).map(jsonArrayToSeq).map(mapToCellHistory)
      val cellHistory = resultObjectToJsonArray(result).map(mapToCellHistory)
      SeqCellHistory(cellHistory)
    }
  }

  private def mapToCellHistory(row: JsonArray): CellHistory = {
    CellHistory(
      row.getLong(0),
      row.getJsonObject(1)
    )
  }
}
