package com.campudus.tableaux.database.domain

import io.circe._
import org.joda.time.DateTime

object Service {

  implicit val dateTimeEncoder: Encoder[DateTime] = Encoder.encodeString.contramap[DateTime](_.toString)

  implicit val encodeMultiLanguageValue: Encoder[MultiLanguageValue[String]] = {
    Encoder.encodeJson.contramap(m =>
      m.values.foldLeft(Json.obj()) {
        case (obj, (langtag, value)) =>
          obj.deepMerge(Json.obj(langtag -> Json.fromString(value)))
    })
  }

  // without a serviceEncoder this error is thrown: "could not find implicit value for parameter encoder: io.circe.Encoder[com.campudus.tableaux.database.domain.Service]"
  implicit val encodeService: Encoder[Service] =
    Encoder
      .forProduct11("id",
                    "type",
                    "name",
                    "ordering",
                    "displayName",
                    "description",
                    "active",
                    "config",
                    "scope",
                    "createdAt",
                    "updatedAt")(
        s =>
          (s.id,
           s.serviceType.toString,
           s.name,
           s.ordering,
           s.displayName,
           s.description,
           s.active,
           s.config,
           s.scope,
           s.createdAt,
           s.updatedAt))

}

// TODO move config and scope into own case classes
case class Service(
    id: Long,
    serviceType: ServiceType,
    name: String,
    ordering: Long,
    displayName: MultiLanguageValue[String],
    description: MultiLanguageValue[String],
    active: Boolean,
    config: JsonObject,
    scope: JsonObject,
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
      case _ => throw new IllegalArgumentException("Invalid argument for ServiceType")
    }
  }
}

case object ServiceTypeAction extends ServiceType {
  override def toString: String = ServiceType.ACTION
}

case object ServiceTypeFilter extends ServiceType {
  override def toString: String = ServiceType.FILTER
}

case object ServiceTypeListener extends ServiceType {
  override def toString: String = ServiceType.LISTENER
}
