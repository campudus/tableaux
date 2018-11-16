package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId, TableId}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery, LanguageType, TableauxDbType}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object RetrieveHistoryModel {

  def apply(connection: DatabaseConnection): RetrieveHistoryModel = {
    new RetrieveHistoryModel(connection)
  }
}

class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieve(tableId: TableId, columnId: ColumnId, rowId: RowId): Future[SeqCellHistory] = {
    val select =
      s"""
         |  SELECT
         |    revision,
         |    event,
         |    column_type,
         |    multilanguage,
         |    author,
         |    timestamp,
         |    value
         |  FROM
         |    user_table_history_$tableId
         |  WHERE
         |    row_id = ?
         |    AND column_id = ?
         |    AND event = 'cell_changed'
         |  ORDER BY
         |    timestamp ASC
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(rowId, columnId))
    } yield {
      val cellHistory = resultObjectToJsonArray(result).map(mapToCellHistory)
      SeqCellHistory(cellHistory)
    }
  }

  private def mapToCellHistory(row: JsonArray): CellHistory = {

    def parseJson(jsonString: String): JsonObject = {
      jsonString match {
        case null => Json.emptyObj()

        case _ =>
          Try(Json.fromObjectString(jsonString)) match {
            case Success(json) => json
            case Failure(_) =>
              logger.error(s"Couldn't parse json. Excepted JSON but got: $jsonString")
              Json.emptyObj()
          }
      }
    }

    CellHistory(
      row.getLong(0),
      row.getString(1),
      TableauxDbType(row.getString(2)),
      LanguageType(Option(row.getString(3))),
      row.getString(4),
      convertStringToDateTime(row.getString(5)),
      parseJson(row.getString(6))
    )
  }
}
