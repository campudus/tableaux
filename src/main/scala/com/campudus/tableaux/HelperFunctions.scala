package com.campudus.tableaux

import org.vertx.scala.core.json.{ JsonObject, JsonArray }
import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._

object HelperFunctions {

  private def asCastedList[A](array: JsonArray): ArgumentCheck[Seq[A]] = {
    import scala.collection.JavaConverters._
    sequence(array.asScala.toList.map(notNull(_, "some array value").flatMap(castElement[A])))
  }

  private def asCastedLong(seq: Seq[Int]): ArgumentCheck[Seq[Long]] = {
    tryMap((x: Seq[Int]) => x.map(_.longValue()), InvalidJsonException(s"Warning: ColumnIds should be Numbers", "invalid"))(seq)
  }

  private def checkNotNullArray(json: JsonObject, field: String): ArgumentCheck[JsonArray] = notNull(json.getArray(field), field)

  private def checkForString(seq: Seq[String]): ArgumentCheck[Seq[String]] = {
    tryMap((y: Seq[String]) => y map { x: String => x }, InvalidJsonException(s"Warning: ColumnNames should be Strings", "invalid"))(seq)
  }

  def toTableauxType(list: Seq[String]): ArgumentCheck[Seq[TableauxDbType]] = {
    sequence(list map (tryMap(Mapper.getDatabaseType, InvalidJsonException("Warning: No such type", "type"))))
  }

  def jsonToSeqOfColumnNameAndType(json: JsonObject): Seq[(String, TableauxDbType)] = {
    (for {
      names <- checkNotNullArray(json, "columnName")
      types <- checkNotNullArray(json, "type")
      namesAsStringList <- asCastedList[String](names)
      typesAsStringList <- asCastedList[String](types)
      namesList <- nonEmpty(namesAsStringList, "columnName")
      checkedNameList <- checkForString(namesList)
      typesList <- nonEmpty(typesAsStringList, "type")
      realTypes <- toTableauxType(typesList)
      zippedList <- checkSameLengthsAndZip(checkedNameList, realTypes)
    } yield zippedList).get
  }

  def jsonToSeqOfRowsWithColumnIdAndValue(json: JsonObject): Seq[Seq[(Long, _)]] = {
    (for {
      columnIds <- checkNotNullArray(json, "columnIds")
      values <- checkNotNullArray(json, "values")
      namesAsJsonArrayList <- asCastedList[JsonArray](columnIds)
      typesAsJsonArrayList <- asCastedList[JsonArray](values)
      columnIdsList <- nonEmpty(namesAsJsonArrayList, "columnIds")
      valuesList <- nonEmpty(typesAsJsonArrayList, "values")
      zippedList <- checkSameLengthsAndZip(columnIdsList, valuesList)
      result <- checkSeq(zippedList)
    } yield result).get
  }

  private def checkSeq(seq: Seq[(JsonArray, JsonArray)]): ArgumentCheck[Seq[Seq[(Long, _)]]] = {
    sequence(seq map { x => jsonArraysToSeq(x) })
  }

  private def jsonArraysToSeq(json: (JsonArray, JsonArray)): ArgumentCheck[Seq[(Long, _)]] = {
    for {
      columnIds <- notNull(json._1, "columnIds")
      values <- notNull(json._2, "values")
      colIdsAsIntList <- asCastedList[Int](columnIds)
      colIdsAsLongList <- asCastedLong(colIdsAsIntList)
      valueAsAnyList <- asCastedList[Any](values)
      colIdList <- nonEmpty(colIdsAsLongList, "columnIds")
      valueList <- nonEmpty(valueAsAnyList, "values")
      zippedList <- checkSameLengthsAndZip(colIdList, valueList)
    } yield zippedList
  }

  def checkTypes(json: JsonObject): Seq[TableauxDbType] = {
    (for {
      types <- checkNotNullArray(json, "type")
      typesAsStringList <- asCastedList[String](types)
      typesList <- nonEmpty(typesAsStringList, "type")
      realTypes <- toTableauxType(typesList)
      checkedTypes <- matchForNormalOrLinkTypes(realTypes)
    } yield checkedTypes).get
  }

  private def matchForNormalOrLinkTypes(seq: Seq[TableauxDbType]): ArgumentCheck[Seq[TableauxDbType]] = {
    seq.head match {
      case LinkType => matchForLinkTypes(seq)
      case _ => matchForNormalTypes(seq)
    }
  }

  private def matchForLinkTypes(seq: Seq[TableauxDbType]): ArgumentCheck[Seq[TableauxDbType]] = {
    sequence(seq map { dbType =>
      dbType match {
        case LinkType => OkArg(dbType)
        case _ => FailArg[TableauxDbType](InvalidJsonException(s"Warning: $dbType is not a LinkType", "link"))
      }
    })
  }

  private def matchForNormalTypes(seq: Seq[TableauxDbType]): ArgumentCheck[Seq[TableauxDbType]] = {
    sequence(seq map { dbType =>
      dbType match {
        case LinkType => FailArg[TableauxDbType](InvalidJsonException(s"Warning: $dbType is a Link, but should be a normal Type", "link"))
        case _ => OkArg(dbType)
      }
    })
  }

}