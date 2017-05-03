package com.campudus.tableaux.database.domain

import org.vertx.scala.core.json._

case class Pagination(offset: Option[Long], limit: Option[Long]) extends DomainObject {

  override def toString: String = {
    val a = offset match {
      case Some(offset) => s"OFFSET $offset"
      case None => ""
    }

    val b = limit match {
      case Some(limit) => s"LIMIT $limit"
      case None => ""
    }

    s"$a $b"
  }

  override def getJson: JsonObject = Json.obj("offset" -> offset.orNull, "limit" -> limit.orNull)
}

case class Page(pagination: Pagination, totalSize: Option[Long]) extends DomainObject {

  override def getJson: JsonObject = pagination.getJson.mergeIn(Json.obj("totalSize" -> totalSize.orNull))
}
