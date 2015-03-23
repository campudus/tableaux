package com.campudus.tableaux

import org.vertx.scala.core.json.{ JsonObject, JsonArray }
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.Tableaux._
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

  private def getLinkInformation(json: JsonObject): ArgumentCheck[Option[LinkConnections]] = for {
    toTable <- notNull(json.getLong("toTable"), "toTable")
    toColumn <- notNull(json.getLong("toColumn"), "toColumn")
    fromColumn <- notNull(json.getLong("fromColumn"), "fromColumn")
  } yield Some(toTable, toColumn, fromColumn)

  private def checkAndGetColumnInfo(seq: Seq[JsonObject]): ArgumentCheck[Seq[CreateColumn]] = for {
    tuples <- sequence(seq map {
      json =>
        for {
          name <- notNull(json.getString("name"), "name")
          kind <- notNull(json.getString("kind"), "kind")
          dbType <- toTableauxType(kind)
          opt <- dbType match {
            case LinkType => getLinkInformation(json)
            case _ => OkArg[Option[LinkConnections]](None)
          }
        } yield {
          val optOrd = Try(json.getNumber("ordering").longValue()).toOption
          CreateColumn(name, dbType, optOrd, opt)
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

  def jsonToSeqOfRowsWithColumnIdAndValue(json: JsonObject): Seq[Seq[(IdType, _)]] = (for {
    rows <- checkNotNullArray(json, "rows")
    columns <- checkNotNullArray(json, "columns")
    rowsAsJsonObjectList <- asCastedList[JsonObject](rows)
    columnsAsJsonObjectList <- asCastedList[JsonObject](columns)
    rowList <- nonEmpty(rowsAsJsonObjectList, "rows")
    columnList <- nonEmpty(columnsAsJsonObjectList, "columns")
    checkedRowList <- checkForJsonObject(rowList)
    checkedColumnList <- checkForJsonObject(columnList)
    realIdTypes <- toIdTypes(checkedColumnList)
    result <- checkSeq(checkedRowList, realIdTypes)
  } yield result).get

  private def toIdTypes(seq: Seq[JsonObject]): ArgumentCheck[Seq[IdType]] = {
    sequence(seq map { json => notNull(json.getNumber("id").longValue(), "id") })
  }

  private def checkSeq(seqOfRows: Seq[JsonObject], seqOfColumnIds: Seq[IdType]): ArgumentCheck[Seq[Seq[(IdType, _)]]] = {
    sequence(seqOfRows map { toValueSeq(_) flatMap (checkSameLengthsAndZip(seqOfColumnIds, _).asInstanceOf[ArgumentCheck[Seq[(IdType, _)]]]) })
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
    sequence(seq map {
      case cc @ CreateColumn(_, LinkType, _, _) => OkArg(cc)
      case cc @ CreateColumn(_, dbType, _, _) => FailArg[CreateColumn](InvalidJsonException(s"Warning: $dbType is not a LinkType", "link"))
    })
  }

  private def matchForNormalTypes(seq: Seq[CreateColumn]): ArgumentCheck[Seq[CreateColumn]] = {
    sequence(seq map {
      case cc @ CreateColumn(_, LinkType, _, _) => FailArg[CreateColumn](InvalidJsonException(s"Warning: Kind is a Link, but should be a normal Type", "link"))
      case cc => OkArg(cc)
    })
  }

  def jsonToValues(json: JsonObject): Any = (for {
    cells <- checkNotNullArray(json, "cells")
    cellsAsJsonObjectList <- asCastedList[JsonObject](cells)
    cellList <- nonEmpty(cellsAsJsonObjectList, "cells")
    checkedCellList <- checkForJsonObject(cellList)
    value <- notNull(checkedCellList(0).getField[Any]("value"), "value")
  } yield value).get

  def getColumnChanges(json: JsonObject): (Option[String], Option[Ordering], Option[TableauxDbType]) = {
    val name = Try(notNull(json.getString("name"), "name").get).toOption
    val ord = Try(json.getNumber("ordering").longValue()).toOption
    val kind = Try(toTableauxType(json.getString("kind")).get).toOption
    (name, ord, kind)
  }

}