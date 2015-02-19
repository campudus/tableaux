package com.campudus.tableaux

import org.vertx.scala.core.json.{ JsonObject, JsonArray }
import com.campudus.tableaux.database.Mapper

object HelperFunctions {

  def jsonToSeqOfRowsWithColumnIdAndValue(json: JsonObject): Seq[Seq[(Long, _)]] = {
    import scala.collection.JavaConverters._
    val columnIds = json.getArray("columnIds").asScala.toSeq.asInstanceOf[Seq[JsonArray]] map { array => array.asScala.toSeq.asInstanceOf[Seq[Int]] map { x => x.asInstanceOf[Long] } }
    val values = json.getArray("values").asScala.toSeq.asInstanceOf[Seq[JsonArray]] map { array => array.asScala.toSeq }

    try {
      (0 until columnIds.length) map { i => (0 until columnIds(i).length) map { j => (columnIds(i)(j), values(i)(j)) } }
    } catch {
      case _: Throwable => throw NotEnoughArgumentsException("Warning: Not enough Arguments", "json")
    }
  }

  def jsonToSeqOfColumnNameAndType(json: JsonObject): Seq[(String, String)] = {
    import scala.collection.JavaConverters._
    val columnName = json.getArray("columnName").asScala.toSeq.asInstanceOf[Seq[String]]
    val columnType = json.getArray("type").asScala.toSeq.asInstanceOf[Seq[String]] map { Mapper.getDatabaseType(_) }

    try {
      (0 until columnName.length) map { i => (columnName(i), columnType(i)) }
    } catch {
      case _: Throwable => throw NotEnoughArgumentsException("Warning: Not enough Arguments", "json")
    }
  }

}