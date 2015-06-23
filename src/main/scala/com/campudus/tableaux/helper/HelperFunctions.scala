package com.campudus.tableaux.helper

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database.model.TableauxModel.{TableId, ColumnId, LinkConnection, Ordering}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{CreateAttachmentColumn, CreateSimpleColumn, CreateLinkColumn, CreateColumn}
import com.campudus.tableaux.{ArgumentCheck, FailArg, InvalidJsonException, OkArg}
import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.util.Try

object HelperFunctions {

  private def asCastedList[A](array: JsonArray): ArgumentCheck[Seq[A]] = {
    import scala.collection.JavaConverters._
    sequence(array.asScala.toList.map(notNull(_, "some array value").flatMap(castElement[A])))
  }

  private def checkNotNullArray(json: JsonObject, field: String): ArgumentCheck[JsonArray] = notNull(json.getArray(field), field)

  private def checkForJsonObject(seq: Seq[JsonObject]): ArgumentCheck[Seq[JsonObject]] = {
    tryMap((y: Seq[JsonObject]) => y map { x: JsonObject => x }, InvalidJsonException(s"Warning: Columns should be in JsonObjects", "object"))(seq)
  }

  def toTableauxType(kind: String): ArgumentCheck[TableauxDbType] = {
    tryMap(Mapper.getDatabaseType, InvalidJsonException("Warning: No such type", "type"))(kind)
  }

  private def getLinkInformation(json: JsonObject): ArgumentCheck[LinkConnection] = for {
    toTable <- notNull(json.getLong("toTable"): TableId, "toTable")
    toColumn <- notNull(json.getLong("toColumn"): ColumnId, "toColumn")
    fromColumn <- notNull(json.getLong("fromColumn"): ColumnId, "fromColumn")
  } yield (toTable, toColumn, fromColumn)

  private def checkAndGetColumnInfo(seq: Seq[JsonObject]): ArgumentCheck[Seq[CreateColumn]] = for {
    tuples <- sequence(seq map {
      json =>
        for {
          name <- notNull(json.getString("name"), "name")
          kind <- notNull(json.getString("kind"), "kind")

          dbType <- toTableauxType(kind)
        } yield {
          val ordering = Try(json.getNumber("ordering").longValue()).toOption

          dbType match {
            case AttachmentType => {
              CreateAttachmentColumn(name, ordering)
            }
            case LinkType => {
              val linkConnections = getLinkInformation(json).get
              CreateLinkColumn(name, ordering, linkConnections)
            }
            case _ => {
              CreateSimpleColumn(name, dbType, ordering)
            }
          }
        }
    })
    checkedDbTypes <- matchForNormalOrLinkTypes(tuples)
  } yield checkedDbTypes

  def jsonToSeqOfColumnNameAndType(json: JsonObject): Seq[CreateColumn] = (for {
    columns <- checkNotNullArray(json, "columns")
    columnsAsJsonObjectList <- asCastedList[JsonObject](columns)
    columnList <- nonEmpty(columnsAsJsonObjectList, "columns")
    checkedColumnList <- checkForJsonObject(columnList)
    seqOfTuples <- checkAndGetColumnInfo(checkedColumnList)
  } yield seqOfTuples).get

  def jsonToSeqOfRowsWithValue(json: JsonObject): Seq[Seq[_]] = (for {
    rows <- checkNotNullArray(json, "rows")
    rowsAsJsonObjectList <- asCastedList[JsonObject](rows)
    rowList <- nonEmpty(rowsAsJsonObjectList, "rows")
    checkedRowList <- checkForJsonObject(rowList)
    result <- sequence(checkedRowList map toValueSeq)
  } yield result).get

  def jsonToSeqOfRowsWithColumnIdAndValue(json: JsonObject): Seq[Seq[(ColumnId, _)]] = (for {
    rows <- checkNotNullArray(json, "rows")
    columns <- checkNotNullArray(json, "columns")
    rowsAsJsonObjectList <- asCastedList[JsonObject](rows)
    columnsAsJsonObjectList <- asCastedList[JsonObject](columns)
    rowList <- nonEmpty(rowsAsJsonObjectList, "rows")
    columnList <- nonEmpty(columnsAsJsonObjectList, "columns")
    checkedRowList <- checkForJsonObject(rowList)
    checkedColumnList <- checkForJsonObject(columnList)
    checkedColumnIdList <- toColumnIds(checkedColumnList)
    result <- checkRowsAndColumns(checkedRowList, checkedColumnIdList)
  } yield result).get

  private def toColumnIds(columns: Seq[JsonObject]): ArgumentCheck[Seq[ColumnId]] = {
    sequence(columns map { json => notNull(json.getNumber("id").longValue(), "id") })
  }

  private def checkRowsAndColumns(rows: Seq[JsonObject], columnIds: Seq[ColumnId]): ArgumentCheck[Seq[Seq[(ColumnId, _)]]] = {
    sequence(rows map { row =>
      toValueSeq(row) flatMap { rowValues =>
        checkSameLengthsAndZip[ColumnId, Any](columnIds, rowValues)
      }
    })
  }

  private def toValueSeq(json: JsonObject): ArgumentCheck[Seq[_]] = for {
    values <- checkNotNullArray(json, "values")
    valueAsAnyList <- asCastedList[Any](values)
    valueList <- nonEmpty(valueAsAnyList, "values")
  } yield valueList

  private def matchForNormalOrLinkTypes(seq: Seq[CreateColumn]): ArgumentCheck[Seq[CreateColumn]] = {
    seq.head.kind match {
      case LinkType => matchForLinkTypes(seq)
      case _ => matchForNormalTypes(seq)
    }
  }

  private def matchForLinkTypes(seq: Seq[CreateColumn]): ArgumentCheck[Seq[CreateColumn]] = {
    sequence(seq map { column => column.kind match {
      case LinkType => OkArg(column)
      case _ => FailArg[CreateColumn](InvalidJsonException(s"Warning: ${column.kind} is not a LinkType", "link"))
    }})
  }

  private def matchForNormalTypes(seq: Seq[CreateColumn]): ArgumentCheck[Seq[CreateColumn]] = {
    sequence(seq map { column => column.kind match {
      case LinkType => FailArg[CreateColumn](InvalidJsonException(s"Warning: Kind is a Link, but should be a normal Type", "link"))
      case _ => OkArg(column)
    }})
  }

  def jsonToValues(json: JsonObject): Any = (for {
    cells <- checkNotNullArray(json, "cells")
    cellsAsJsonObjectList <- asCastedList[JsonObject](cells)
    cellList <- nonEmpty(cellsAsJsonObjectList, "cells")
    checkedCellList <- checkForJsonObject(cellList)
    value <- notNull(checkedCellList.head.getField[Any]("value"), "value")
  } yield value).get

  def getColumnChanges(json: JsonObject): (Option[String], Option[Ordering], Option[TableauxDbType]) = {
    val name = Try(notNull(json.getString("name"), "name").get).toOption
    val ord = Try(json.getNumber("ordering").longValue()).toOption
    val kind = Try(toTableauxType(json.getString("kind")).get).toOption
    (name, ord, kind)
  }
}
