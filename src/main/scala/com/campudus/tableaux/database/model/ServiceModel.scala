package com.campudus.tableaux.database.model

import com.campudus.tableaux.database.domain.{MultiLanguageValue, Service}
import com.campudus.tableaux.database.model.ServiceModel.{Ordering, ServiceId}
import com.campudus.tableaux.database.{DatabaseConnection, DatabaseQuery}
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{Json, JsonArray}

import scala.concurrent.Future

object ServiceModel {
  type ServiceId = Long
  type Ordering = Long

  def apply(connection: DatabaseConnection): ServiceModel = {
    new ServiceModel(connection)
  }
}

class ServiceModel(override protected[this] val connection: DatabaseConnection) extends DatabaseQuery {
  val table: String = "system_services"

  private def selectStatement(conditions: Option[String]): String = {
    val where = if (conditions.isDefined) {
      s"WHERE ${conditions.get}"
    } else {
      ""
    }

    s"""SELECT
       |  id,
       |  type,
       |  name,
       |  ordering,
       |  displayname,
       |  description,
       |  active,
       |  config,
       |  scope,
       |  created_at,
       |  updated_at
       |FROM $table $where ORDER BY name""".stripMargin
  }

  def retrieve(id: ServiceId): Future[Service] = {
    for {
      result <- connection.query(selectStatement(Some("id = ?")), Json.arr(id.toString))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToService(resultArr.head)
    }
  }

  def retrieveAll(): Future[Seq[Service]] = {
    for {
      result <- connection.query(selectStatement(None))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToService)
    }
  }

  private def convertJsonArrayToService(arr: JsonArray): Service = {

    Service(
      arr.get[ServiceId](0), // id
      arr.get[String](1), // type
      arr.get[String](2), // name
      arr.get[Ordering](3), // ordering
      MultiLanguageValue.fromString(arr.get[String](4)), // displayname
      MultiLanguageValue.fromString(arr.get[String](5)), // description
      arr.get[Boolean](6), // active
//      arr.get[String](7), // config
//      arr.get[String](8), // scope
      convertStringToDateTime(arr.get[String](9)), // created_at
      convertStringToDateTime(arr.get[String](10)) // updated_at
    )
  }
}
