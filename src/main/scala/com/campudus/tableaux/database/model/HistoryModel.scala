package com.campudus.tableaux.database.model

import com.campudus.tableaux.controller.SystemController
import com.campudus.tableaux.{InvalidRequestException, UnprocessableEntityException}
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.tableaux.RetrieveRowModel
import com.campudus.tableaux.helper.IdentifierFlattener
import com.campudus.tableaux.helper.ResultChecker._
import io.vertx.core.json.JsonObject
import org.vertx.scala.core.json._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class RetrieveHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  def retrieve(table: Table,
               column: ColumnType[_],
               rowId: RowId,
               langtagOpt: Option[String]): Future[SeqCellHistory] = {

    val whereLanguage = (column.languageType, langtagOpt) match {
      case (MultiLanguage, Some(langtag)) => s" AND (value -> 'value' -> '$langtag')::json IS NOT NULL"
      case (_, None) => ""
      case (_, Some(_)) =>
        throw new InvalidRequestException(
          "History values filtered by langtags can only be retrieved from multi-language columns")
    }

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
         |    user_table_history_${table.id}
         |  WHERE
         |    row_id = ?
         |    AND column_id = ?
         |    AND event = 'cell_changed'
         |    $whereLanguage
         |  ORDER BY
         |    timestamp ASC
         """.stripMargin

    for {
      result <- connection.query(select, Json.arr(rowId, column.id))
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

case class CreateHistoryModel(protected[this] val connection: DatabaseConnection) extends DatabaseQuery {

  val systemModel = SystemModel(connection)

  def create(
      table: Table,
      rowId: RowId,
      values: Seq[(ColumnType[_], _)],
      retrieveCellFn: (Table, ColumnId, RowId) => Future[Cell[Any]],
      replace: Boolean = false
  ): Future[Unit] = {

    // TODO if we put/replace the values we must not merge old and new values, but just replace the values

    ColumnType.splitIntoTypesWithValues(values) match {
      case Failure(ex) =>
        Future.failed(ex)

      case Success((simples, multis, links, attachments)) =>
        for {
          _ <- if (simples.isEmpty) Future.successful(()) else createSimple(table, rowId, simples)
          _ <- if (multis.isEmpty) Future.successful(()) else createTranslation(table, rowId, multis)
          _ <- if (links.isEmpty) Future.successful(()) else createLinks(table, rowId, links, retrieveCellFn, replace)
//          _ <- if (attachments.isEmpty) Future.successful(()) else createSimple(table, columnId, rowId, simple)
        } yield ()
    }
  }

  private def createLinks(
      table: Table,
      rowId: RowId,
      links: Seq[(LinkColumn, Seq[RowId])],
      retrieveCellFn: (Table, ColumnId, RowId) => Future[Cell[Any]],
      replace: Boolean
  ): Future[Unit] = {

    def wrapValue(valueSeq: Seq[(_, RowId, Object)]): JsonObject = {
      Json.obj(
        "value" ->
          valueSeq.map({
            case (_, rowId, value) => Json.obj("id" -> rowId, "value" -> value)
          })
      )
    }

    def retrieveSystemLangTags(): Future[Seq[String]] = {
      import scala.collection.JavaConverters._
      systemModel
        .retrieveSetting(SystemController.SETTING_LANGTAGS)
        .map(
          valueOpt =>
            valueOpt
              .map(f => Json.fromArrayString(f).asScala.map({ case str: String => str }).toSeq)
              .getOrElse(Seq.empty[String])
        )
    }

    val futureSequence = links.map({
      case (col, linkIds) => {
        if (linkIds.isEmpty) {
          println("Just insert [] into history")
          insertHistory(table, rowId, col.id, col.kind, col.languageType, wrapValue(Seq()))
        } else {
          val linkedCellSeq = linkIds.map(linkId =>
            for {
              // in some cases we are too fast, so the links could NOT be inserted yet
              foreignIdentifier <- retrieveCellFn(col.to.table, col.to.id, linkId)
            } yield foreignIdentifier)

          for {
            cellSeq <- Future.sequence(linkedCellSeq)
            langTags <- retrieveSystemLangTags
          } yield {
            val preparedData = cellSeq.map(cell => {
              IdentifierFlattener.compress(langTags, cell.value) match {
                case Right(v) => (MultiLanguage, cell.rowId, v)
                case Left(v) => (LanguageNeutral, cell.rowId, v)
              }
            })

            val languageType = preparedData.head._1

            insertHistory(table, rowId, col.id, col.kind, languageType, wrapValue(preparedData))
          }
        }
      }
    })

    Future.sequence(futureSequence).map(_ => ())
  }

  private def createTranslation(
      table: Table,
      rowId: RowId,
      values: Seq[(SimpleValueColumn[_], Map[String, Option[_]])]
  ): Future[List[RowId]] = {

    def wrapLanguageValue(langtag: String, value: Any): JsonObject = Json.obj("value" -> Json.obj(langtag -> value))

    val entries = for {
      (column, langtagValueOptMap) <- values
      (langtag: String, valueOpt) <- langtagValueOptMap
    } yield (langtag, (column, valueOpt))

    val columnsForLang = entries
      .groupBy({ case (langtag, _) => langtag })
      .mapValues(_.map({ case (_, columnValueOpt) => columnValueOpt }))

    val futureSequence: List[Future[RowId]] = columnsForLang.toList
      .flatMap({
        case (langtag, columnValueOptSeq) =>
          val languageCellEntries = columnValueOptSeq
            .map({
              case (column: SimpleValueColumn[_], valueOpt) =>
                (column.id, column.kind, column.languageType, wrapLanguageValue(langtag, valueOpt.orNull))
            })

          languageCellEntries.map({
            case (columnId, columnType, languageType, value) =>
              insertHistory(table, rowId, columnId, columnType, languageType, value)
          })
      })

    Future.sequence(futureSequence)
  }

  private def createSimple(
      table: Table,
      rowId: RowId,
      simples: List[(SimpleValueColumn[_], Option[Any])]
  ): Future[List[RowId]] = {

    def wrapValue(value: Any): JsonObject = Json.obj("value" -> value)

    val cellEntries = simples
      .map({
        case (column: SimpleValueColumn[_], valueOpt) =>
          (column.id, column.kind, column.languageType, wrapValue(valueOpt.orNull))
      })

    val futureSequence: List[Future[RowId]] = cellEntries
      .map({
        case (columnId, columnType, languageType, value) =>
          insertHistory(table, rowId, columnId, columnType, languageType, value)
      })

    Future.sequence(futureSequence)
  }

  private def insertHistory(
      table: Table,
      rowId: RowId,
      columnId: ColumnId,
      columnType: TableauxDbType,
      languageType: LanguageType,
      json: JsonObject
  ): Future[RowId] = {

    logger.info(s"createHistory ${table.id} $columnId $rowId $json")

    for {
      result <- connection.query(
        s"""INSERT INTO
           |  user_table_history_${table.id}
           |    (row_id, column_id, column_type, multilanguage, value)
           |  VALUES
           |    (?, ?, ?, ?, ?)
           |  RETURNING revision""".stripMargin,
        Json.arr(rowId, columnId, columnType.toString, languageType.toString, json.toString)
      )
    } yield {
      insertNotNull(result).head.get[RowId](0)
    }
  }
}
