package com.campudus.tableaux.database.model

import com.campudus.tableaux.ShouldBeUniqueException
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.ServiceModel.ServiceId
import com.campudus.tableaux.database.model.TableauxModel.Ordering
import com.campudus.tableaux.helper.ResultChecker._
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

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

  def create(
      name: String,
      serviceType: String,
      ordering: Option[Long],
      displayName: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      active: Boolean,
      config: Option[JsonObject],
      scope: Option[JsonObject]
  ): Future[ServiceId] = {

    val insert = s"""INSERT INTO $table (
                    |  name,
                    |  type,
                    |  ordering,
                    |  displayname,
                    |  description,
                    |  active,
                    |  config,
                    |  scope)
                    |VALUES
                    |  (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""".stripMargin

    for {
      _ <- checkUniqueName(name)

      result <- connection.query(
        insert,
        Json
          .arr(
            name,
            serviceType,
            ordering.orNull,
            displayName.getJson.toString,
            description.getJson.toString,
            active,
            config.map(_.toString).orNull,
            scope.map(_.toString).orNull
          )
      )

      serviceId = insertNotNull(result).head.get[ServiceId](0)
    } yield serviceId

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

  private def checkUniqueName(name: String): Future[Unit] = {

    val sql = s"SELECT COUNT(*) = 0 FROM $table WHERE name = ?"
    connection
      .selectSingleValue[Boolean](sql, Json.arr(name))
      .flatMap({
        case true => Future.successful(())
        case false =>
          Future.failed(new ShouldBeUniqueException(s"Name of service should be unique $name.", "service"))
      })
  }
}
