package com.campudus.tableaux.database.domain

import io.circe.Encoder
import org.joda.time.DateTime

object Service {

  implicit val dateTimeEncoder: Encoder[DateTime] = Encoder.encodeString.contramap[DateTime](_.toString)

  // without a serviceEncoder this error is thrown: "could not find implicit value for parameter encoder: io.circe.Encoder[com.campudus.tableaux.database.domain.Service]"
  implicit val encodeService: Encoder[Service] =
    Encoder
      .forProduct9("id", "type", "name", "ordering", "displayName", "description", "active", "createdAt", "updatedAt")(
        s =>
          (s.id, s.serviceType, s.name, s.ordering, s.displayName, s.description, s.active, s.createdAt, s.updatedAt))

}

case class Service(
    id: Long,
    serviceType: String,
    name: String,
    ordering: Long,
    displayName: Option[String],
    description: Option[String],
    active: Boolean,
    createdAt: Option[DateTime],
    updatedAt: Option[DateTime]
)

case class ServiceSeq(services: Seq[Service])

trait ServiceType

object ServiceType {
  final val ACTION = "action"
  final val FILTER = "filter"
  final val LISTENER = "listener"

  def apply(typeOption: Option[String]): ServiceType = {
    typeOption match {
      case Some(ACTION) => ServiceTypeAction
      case Some(FILTER) => ServiceTypeFilter
      case Some(LISTENER) => ServiceTypeListener
      case _ => throw new IllegalArgumentException("Invalid argument for ServiceType.apply")
    }
  }
}

case object ServiceTypeAction extends ServiceType

case object ServiceTypeFilter extends ServiceType

case object ServiceTypeListener extends ServiceType
