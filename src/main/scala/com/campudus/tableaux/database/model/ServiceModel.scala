package com.campudus.tableaux.database.model

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.ServiceModel.ServiceId
import com.campudus.tableaux.database.model.TableauxModel.Ordering
import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}
import com.campudus.tableaux.ShouldBeUniqueException
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future
import io.vertx.scala.ext.web.RoutingContext

object ServiceModel {
  type ServiceId = Long
  type Ordering = Long

  def apply(connection: DatabaseConnection)(
      implicit roleModel: RoleModel
  ): ServiceModel = {
    new ServiceModel(connection)
  }
}

class ServiceModel(override protected[this] val connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {
  val table: String = "system_services"

  def update(
      serviceId: ServiceId,
      name: Option[String],
      serviceType: Option[ServiceType],
      ordering: Option[Ordering],
      displayName: Option[MultiLanguageValue[String]],
      description: Option[MultiLanguageValue[String]],
      active: Option[Boolean],
      config: Option[JsonObject],
      scope: Option[JsonObject]
  )(implicit user: TableauxUser): Future[Unit] = {

    val updateParamOpts = Map(
      "name" -> name,
      "type" -> serviceType,
      "ordering" -> ordering,
      "displayName" -> displayName,
      "description" -> description,
      "active" -> active,
      "config" -> config,
      "scope" -> scope
    )

    val paramsToUpdate = updateParamOpts
      .filter({ case (_, v) => v.isDefined })
      .map({ case (k, v) => (k, v.get) })

    val columnString2valueString: Map[String, String] = paramsToUpdate.map({
      case (columnName, value) =>
        val columnString = s"$columnName = ?"

        val valueString = value match {
          case m: MultiLanguageValue[_] => m.getJson.toString
          case a => a.toString
        }

        columnString -> valueString
    })

    val columnsString = columnString2valueString.keys.mkString(", ")
    val update = s"UPDATE $table SET $columnsString, updated_at = CURRENT_TIMESTAMP WHERE id = ?"

    val binds = Json.arr(columnString2valueString.values.toSeq: _*).add(serviceId.toString)

    for {
      _ <- name.map(checkUniqueName).getOrElse(Future.successful(()))
      _ <- connection.query(update, binds)
    } yield ()
  }

  private def selectStatement(condition: Option[String]): String = {

    val where = condition.map(cond => s"WHERE $cond").getOrElse("")

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
       |FROM $table $where
       |ORDER BY ordering""".stripMargin
  }

  def retrieve(id: ServiceId)(implicit user: TableauxUser): Future[Service] = {
    for {
      result <- connection.query(selectStatement(Some("id = ?")), Json.arr(id.toString))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToService(resultArr.head)
    }
  }

  def delete(id: ServiceId): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE id = ?"

    for {
      result <- connection.query(delete, Json.arr(id))
      _ <- Future(deleteNotNull(result))
    } yield ()
  }

  def create(
      name: String,
      serviceType: ServiceType,
      ordering: Option[Long],
      displayName: MultiLanguageValue[String],
      description: MultiLanguageValue[String],
      active: Boolean,
      config: Option[JsonObject],
      scope: Option[JsonObject]
  )(implicit user: TableauxUser): Future[ServiceId] = {

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
            serviceType.toString,
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

  def retrieveAll()(implicit user: TableauxUser): Future[Seq[Service]] = {
    for {
      result <- connection.query(selectStatement(None))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToService)
    }
  }

  private def convertJsonArrayToService(arr: JsonArray)(implicit user: TableauxUser): Service = {
    val config = JsonUtils.parseJson(arr.get[String](7))
    val scope = JsonUtils.parseJson(arr.get[String](8))

    Service(
      arr.get[ServiceId](0), // id
      ServiceType(Option(arr.get[String](1))), // type
      arr.get[String](2), // name
      arr.get[Ordering](3), // ordering
      MultiLanguageValue.fromString(arr.get[String](4)), // displayname
      MultiLanguageValue.fromString(arr.get[String](5)), // description
      arr.get[Boolean](6), // active
      config, // config
      scope, // scope
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
        case false => Future.failed(ShouldBeUniqueException(s"Name of service should be unique $name.", "service"))
      })
  }
}
