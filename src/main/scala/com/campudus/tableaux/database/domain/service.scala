package com.campudus.tableaux.database.domain

import com.campudus.tableaux.RequestContext
import com.campudus.tableaux.router.auth.permission.{RoleModel, ScopeServiceSeq, ScopeTableSeq}
import org.joda.time.DateTime
import org.vertx.scala.core.json._

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
) extends DomainObject {

  override def getJson: JsonObject = {
    Json.obj(
      "id" -> id,
      "type" -> serviceType.toString,
      "name" -> name,
      "ordering" -> ordering,
      "displayName" -> displayName.getJson,
      "description" -> description.getJson,
      "active" -> active,
      "config" -> config,
      "scope" -> scope,
      "createdAt" -> optionToString(createdAt),
      "updatedAt" -> optionToString(updatedAt)
    )
  }
}

case class ServiceSeq(services: Seq[Service])(
    implicit requestContext: RequestContext,
    roleModel: RoleModel
) extends DomainObject {

  override def getJson: JsonObject = {
    val serviceSeqJson = Json.obj("services" -> services.map(_.getJson))
    roleModel.enrichDomainObject(serviceSeqJson, ScopeServiceSeq)
  }
}

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
