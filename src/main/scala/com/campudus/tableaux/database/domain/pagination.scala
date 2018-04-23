package com.campudus.tableaux.database.domain

import com.campudus.tableaux.{ArgumentCheck, ArgumentChecker, OkArg}
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

  def check: ArgumentCheck[Pagination] = {

    def greaterThan(opt: Option[Long], than: Long, name: String): ArgumentCheck[Option[Long]] = {
      opt match {
        case Some(value) => ArgumentChecker.greaterThan(value, than, name).map(v => Option(v))
        case None => OkArg(None)
      }
    }

    for {
      _ <- greaterThan(offset, -1, "offset")
      _ <- greaterThan(limit, -1, "limit")
    } yield this
  }

}

case class Page(pagination: Pagination, totalSize: Option[Long]) extends DomainObject {

  override def getJson: JsonObject = pagination.getJson.mergeIn(Json.obj("totalSize" -> totalSize.orNull))
}
