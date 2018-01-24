package com.campudus.tableaux.database.domain

import java.util.UUID

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, RowId}
import org.joda.time.DateTime
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.collection.JavaConverters._

case class RowLevelAnnotations(finalFlag: Boolean) extends DomainObject {

  def isDefined: Boolean = finalFlag

  override def getJson: JsonObject = {
    Json.obj(
      "final" -> finalFlag
    )
  }
}

object CellAnnotationType {

  final val ERROR = "error"
  final val WARNING = "warning"
  final val INFO = "info"
  final val FLAG = "flag"

  def apply(annotationName: String): CellAnnotationType = {
    annotationName match {
      case CellAnnotationType.ERROR => ErrorAnnotationType
      case CellAnnotationType.WARNING => WarningAnnotationType
      case CellAnnotationType.INFO => InfoAnnotationType
      case CellAnnotationType.FLAG => FlagAnnotationType

      case _ => throw new IllegalArgumentException(s"Invalid cell annotation $annotationName")
    }
  }
}

sealed trait CellAnnotationType {

  def toString: String
}

case object ErrorAnnotationType extends CellAnnotationType {

  override def toString: String = CellAnnotationType.ERROR
}

case object WarningAnnotationType extends CellAnnotationType {

  override def toString: String = CellAnnotationType.WARNING
}

case object InfoAnnotationType extends CellAnnotationType {

  override def toString: String = CellAnnotationType.INFO
}

case object FlagAnnotationType extends CellAnnotationType {

  override def toString: String = CellAnnotationType.FLAG
}

object CellLevelAnnotations {

  def apply(columns: Seq[ColumnType[_]], annotationsAsJsonArray: JsonArray): CellLevelAnnotations = {
    val annotations = annotationsAsJsonArray.asScala.toSeq
      .map({
        case obj: JsonObject =>
          val columnId = obj.getLong("column_id").longValue()
          obj.remove("column_id")

          val uuid = obj.getString("uuid")
          val langtags = obj.getJsonArray("langtags", Json.emptyArr()).asScala.map(_.toString).toList
          val annotationType = CellAnnotationType(obj.getString("type"))
          val value = obj.getString("value")
          val createdAt = DateTime.parse(obj.getString("createdAt"))

          (columnId, CellLevelAnnotation(UUID.fromString(uuid), annotationType, langtags, value, createdAt))
      })
      .groupBy({
        case (columnId, _) => columnId
      })
      .map({
        case (columnId, annotationsAsTupleSeq) => (columnId, annotationsAsTupleSeq.map(_._2))
      })

    CellLevelAnnotations(columns, annotations)
  }
}

case class CellLevelAnnotation(
    uuid: UUID,
    annotationType: CellAnnotationType,
    langtags: Seq[String],
    value: String,
    createdAt: DateTime
) extends DomainObject {

  override def getJson: JsonObject = {
    val json = Json.obj(
      "uuid" -> uuid.toString,
      "type" -> annotationType.toString,
      "value" -> value,
      "createdAt" -> createdAt.toString()
    )

    if (langtags.nonEmpty) {
      json.put("langtags", compatibilityGet(langtags))
    }

    json
  }
}

case class CellLevelAnnotations(columns: Seq[ColumnType[_]], annotations: Map[ColumnId, Seq[CellLevelAnnotation]])
    extends DomainObject {

  def isDefined: Boolean = annotations.values.exists(_.nonEmpty)

  override def getJson: JsonObject = {
    val seqOpt = columns.map(column => annotations.get(column.id))

    Json.obj("annotations" -> compatibilityGet(seqOpt))
  }
}

case class TableWithCellAnnotations(table: Table, annotations: Map[RowId, Map[ColumnId, Seq[CellLevelAnnotation]]])
    extends DomainObject {

  override def getJson: JsonObject = {
    val rows = annotations.seq.map({
      case (rowId, annotationsByRow) =>
        val columns = annotationsByRow.seq.map({
          case (columnId, annotationsByColumn) =>
            Json.obj(
              "id" -> columnId,
              "annotations" -> annotationsByColumn.map(_.getJson)
            )
        })

        Json.obj(
          "id" -> rowId,
          "annotationsByColumns" -> columns
        )
    })

    table.getJson.mergeIn(Json.obj("annotationsByRows" -> rows))
  }
}

case class CellAnnotationCount(annotationType: CellAnnotationType,
                               value: Option[String],
                               langtag: Option[String],
                               count: Long,
                               lastCreatedAt: DateTime)
    extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj(
      "type" -> annotationType.toString,
      "value" -> value.orNull,
      "langtag" -> langtag.orNull,
      "count" -> count,
      "lastCreatedAt" -> lastCreatedAt.toString()
    )
  }
}

case class TableWithCellAnnotationCount(table: Table, totalSize: Long, annotationCount: Seq[CellAnnotationCount])
    extends DomainObject {

  override def getJson: JsonObject = {
    table.getJson.mergeIn(Json.obj("totalSize" -> totalSize, "annotationCount" -> compatibilityGet(annotationCount)))
  }
}
