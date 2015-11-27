package com.campudus.tableaux.helper

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering, TableId}
import com.campudus.tableaux.{ArgumentCheck, InvalidJsonException}
import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.util.Try

object JsonUtils {

  private def asCastedList[A](array: JsonArray): ArgumentCheck[Seq[A]] = {
    import scala.collection.JavaConverters._
    sequence(array.asScala.toList.map(notNull(_, "some array value").flatMap(tryCast[A])))
  }

  private def checkNotNullArray(json: JsonObject, field: String): ArgumentCheck[JsonArray] = notNull(json.getJsonArray(field), field)

  private def checkForJsonObject(seq: Seq[JsonObject]): ArgumentCheck[Seq[JsonObject]] = {
    tryMap((y: Seq[JsonObject]) => y map { x: JsonObject => x }, InvalidJsonException(s"Warning: Array should only contain JsonObjects", "object"))(seq)
  }

  private def toTableauxType(kind: String): ArgumentCheck[TableauxDbType] = {
    tryMap(Mapper.getDatabaseType, InvalidJsonException("Warning: No such type", "type"))(kind)
  }

  private def toLinkConnection(json: JsonObject): ArgumentCheck[LinkConnection] = for {
    toTable <- notNull(json.getLong("toTable"): TableId, "toTable")
    toColumn <- notNull(json.getLong("toColumn"): ColumnId, "toColumn")
    fromColumn <- notNull(json.getLong("fromColumn"): ColumnId, "fromColumn")
  } yield LinkConnection(toTable, toColumn, fromColumn)

  private def toJsonObjectSeq(field: String, json: JsonObject): ArgumentCheck[Seq[JsonObject]] = {
    for {
      columns <- checkNotNullArray(json, field)
      columnsAsJsonObjectList <- asCastedList[JsonObject](columns)
      columnList <- nonEmpty(columnsAsJsonObjectList, field)
      checkedColumnList <- checkForJsonObject(columnList)
    } yield checkedColumnList
  }

  def toCreateColumnSeq(json: JsonObject): Seq[CreateColumn] = (for {
    seq <- toJsonObjectSeq("columns", json)
    tuples <- sequence(seq map {
      json =>
        for {
          name <- notNull(json.getString("name"), "name")
          kind <- notNull(json.getString("kind"), "kind")

          dbType <- toTableauxType(kind)
        } yield {
          val toName = Try(Option(json.getString("toName"))).toOption.flatten

          val ordering = Try(json.getInteger("ordering").longValue()).toOption
          val multilanguage = Try[Boolean](json.getBoolean("multilanguage")).getOrElse(false)

          dbType match {
            case AttachmentType => CreateAttachmentColumn(name, ordering)
            case LinkType => CreateLinkColumn(name, ordering, toLinkConnection(json).get, toName)
            case _ => CreateSimpleColumn(name, ordering, dbType, LanguageType(multilanguage))
          }
        }
    })
  } yield tuples).get

  def toRowValueSeq(json: JsonObject): Seq[Seq[_]] = (for {
    checkedRowList <- toJsonObjectSeq("rows", json)
    result <- sequence(checkedRowList map toValueSeq)
  } yield result).get

  def toColumnValueSeq(json: JsonObject): Seq[Seq[(ColumnId, _)]] = (for {
    columns <- toJsonObjectSeq("columns", json).map(columns => sequence(columns.map(hasLong("id", _))))
    rows <- toJsonObjectSeq("rows", json)
    result <- mergeColumnWithValue(columns.get, rows)
  } yield result).get

  private def mergeColumnWithValue(columns: Seq[ColumnId], rows: Seq[JsonObject]): ArgumentCheck[Seq[Seq[(ColumnId, _)]]] = {
    sequence(rows map { row =>
      toValueSeq(row) flatMap { values =>
        checkSameLengthsAndZip[ColumnId, Any](columns, values)
      }
    })
  }

  def toTupleSeq[A](json: JsonObject): Seq[(String, A)] = {
    import scala.collection.JavaConverters._

    val fields = json.fieldNames().asScala.toSeq
    fields.map(field => (field, json.getValue(field).asInstanceOf[A]))
  }

  private def toValueSeq(json: JsonObject): ArgumentCheck[Seq[Any]] = for {
    values <- checkNotNullArray(json, "values")
    valueAsAnyList <- asCastedList[Any](values)
    valueList <- nonEmpty(valueAsAnyList, "values")
  } yield valueList

  def toColumnChanges(json: JsonObject): (Option[String], Option[Ordering], Option[TableauxDbType]) = {
    val name = Try(notNull(json.getString("name"), "name").get).toOption
    val ord = Try(json.getInteger("ordering").longValue()).toOption
    val kind = Try(toTableauxType(json.getString("kind")).get).toOption
    (name, ord, kind)
  }
}
