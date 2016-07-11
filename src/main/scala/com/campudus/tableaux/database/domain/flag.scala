package com.campudus.tableaux.database.domain

import org.vertx.scala.core.json._

object RowLevelFlags {
  def apply(finalFlag: Boolean, needsTranslationFlags: JsonArray): RowLevelFlags = {
    import scala.collection.JavaConverters._
    val seq = needsTranslationFlags.asScala.toSeq.map({ case langtag: String => langtag })

    RowLevelFlags(finalFlag, seq)
  }
}

case class RowLevelFlags(finalFlag: Boolean, needsTranslationFlags: Seq[String]) extends DomainObject {
  override def getJson: JsonObject = Json.obj(
    "final" -> finalFlag,
    "needsTranslation" -> compatibilityGet(needsTranslationFlags)
  )
}