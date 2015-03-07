package com.campudus.tableaux

import org.vertx.scala.core.json.{ JsonObject, JsonArray }
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.TableStructure._

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

  private def getLinkInformation(json: JsonObject): ArgumentCheck[Option[(IdType, IdType, IdType)]] = for {
    toTable <- notNull(json.getLong("toTable"), "toTable")
    toColumn <- notNull(json.getLong("toColumn"), "toColumn")
    fromColumn <- notNull(json.getLong("fromColumn"), "fromColumn")
  } yield Some(toTable, toColumn, fromColumn)

  private def seqOfJsonObjectsToTuples(seq: Seq[JsonObject]): ArgumentCheck[Seq[(String, TableauxDbType, Option[(IdType, IdType, IdType)])]] = for {
    tuples <- sequence(seq map {
      json =>
        for {
          name <- notNull(json.getString("name"), "name")
          kind <- notNull(json.getString("kind"), "kind")
          dbType <- toTableauxType(kind)
          opt <- (dbType match {
            case LinkType => getLinkInformation(json)
            case _ => OkArg(None)
          }).asInstanceOf[ArgumentCheck[Option[(IdType, IdType, IdType)]]]
        } yield (name, dbType, opt)
    })
    checkedDbTypes <- matchForNormalOrLinkTypes(tuples)
  } yield checkedDbTypes

  def jsonToSeqOfColumnNameAndType(json: JsonObject): Seq[(String, TableauxDbType, Option[(IdType, IdType, IdType)])] = (for {
    columns <- checkNotNullArray(json, "columns")
    columnsAsJsonObjectList <- asCastedList[JsonObject](columns)
    columnList <- nonEmpty(columnsAsJsonObjectList, "columns")
    checkedColumnList <- checkForJsonObject(columnList)
    seqOfTuples <- seqOfJsonObjectsToTuples(checkedColumnList)
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

  private def matchForNormalOrLinkTypes[A](seq: Seq[(String, TableauxDbType, A)]): ArgumentCheck[Seq[(String, TableauxDbType, A)]] = {
    seq.head match {
      case (_, LinkType, _) => matchForLinkTypes(seq)
      case _ => matchForNormalTypes(seq)
    }
  }

  private def matchForLinkTypes[A](seq: Seq[(String, TableauxDbType, A)]): ArgumentCheck[Seq[(String, TableauxDbType, A)]] = {
    sequence(seq map {
      case (name, LinkType, opt) => OkArg[(String, TableauxDbType, A)](name, LinkType, opt)
      case (_, dbType, _) => FailArg[(String, TableauxDbType, A)](InvalidJsonException(s"Warning: $dbType is not a LinkType", "link"))
    })
  }

  private def matchForNormalTypes[A](seq: Seq[(String, TableauxDbType, A)]): ArgumentCheck[Seq[(String, TableauxDbType, A)]] = {
    sequence(seq map {
      case (_, LinkType, _) => FailArg[(String, TableauxDbType, A)](InvalidJsonException(s"Warning: Kind is a Link, but should be a normal Type", "link"))
      case (name, dbType, opt) => OkArg(name, dbType, opt)
    })
  }

  def jsonToValues(json: JsonObject): Any = (for {
    cells <- checkNotNullArray(json, "cells")
    cellsAsJsonObjectList <- asCastedList[JsonObject](cells)
    cellList <- nonEmpty(cellsAsJsonObjectList, "cells")
    checkedCellList <- checkForJsonObject(cellList)
    value <- notNull(checkedCellList(0).getField[Any]("value"), "value")
  } yield value).get

}