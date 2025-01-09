package com.campudus.tableaux.database.model

import com.campudus.tableaux.ShouldBeUniqueException
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.helper.JsonUtils
import com.campudus.tableaux.helper.ResultChecker._
import com.campudus.tableaux.router.auth.permission.{RoleModel, TableauxUser}

import io.vertx.scala.ext.web.RoutingContext
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import scala.concurrent.Future

object CellAnnotationConfigModel {

  def apply(connection: DatabaseConnection)(
      implicit roleModel: RoleModel
  ): CellAnnotationConfigModel = {
    new CellAnnotationConfigModel(connection)
  }
}

class CellAnnotationConfigModel(override protected[this] val connection: DatabaseConnection)(
    implicit roleModel: RoleModel
) extends DatabaseQuery {
  val table: String = "system_annotations"

  def update(
      name: String,
      priority: Option[Int],
      fgColor: Option[String],
      bgColor: Option[String],
      displayName: Option[MultiLanguageValue[String]],
      isMultilang: Option[Boolean],
      isDashboard: Option[Boolean]
  )(implicit user: TableauxUser): Future[CellAnnotationConfig] = {

    val updateParamOpts = Map(
      "priority" -> priority,
      "fg_color" -> fgColor,
      "bg_color" -> bgColor,
      "display_name" -> displayName,
      "is_multilang" -> isMultilang,
      "is_dashboard" -> isDashboard
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
    val update = s"UPDATE $table SET $columnsString WHERE name = ?"

    val binds = Json.arr(columnString2valueString.values.toSeq: _*).add(name)

    for {
      _ <- connection.query(update, binds)
      annotation <- retrieve(name)
    } yield annotation
  }

  private def selectStatement(condition: Option[String]): String = {

    val where = condition.map(cond => s"WHERE $cond").getOrElse("")

    s"""SELECT
       |  name,
       |  priority,
       |  fg_color,
       |  bg_color,
       |  display_name,
       |  is_multilang,
       |  is_dashboard,
       |  is_custom
       |FROM $table $where
       |ORDER BY priority""".stripMargin
  }

  def retrieve(name: String)(implicit user: TableauxUser): Future[CellAnnotationConfig] = {
    for {
      result <- connection.query(selectStatement(Some("name = ?")), Json.arr(name))
      resultArr <- Future(selectNotNull(result))
    } yield {
      convertJsonArrayToCellAnnotationConfig(resultArr.head)
    }
  }

  def delete(name: String): Future[Unit] = {
    val delete = s"DELETE FROM $table WHERE name = ?"

    for {
      result <- connection.query(delete, Json.arr(name))
      _ <- Future(deleteNotNull(result))
    } yield ()
  }

  def create(
      name: String,
      priority: Option[Int],
      fgColor: String,
      bgColor: String,
      displayName: MultiLanguageValue[String],
      isMultilang: Boolean,
      isDashboard: Boolean
  )(implicit user: TableauxUser): Future[String] = {

    val insert = s"""INSERT INTO $table (
                    |  name,
                    |  priority,
                    |  fg_color,
                    |  bg_color,
                    |  display_name,
                    |  is_multilang,
                    |  is_dashboard)
                    |VALUES
                    |  (?, ?, ?, ?, ?, ?, ?) RETURNING name""".stripMargin

    for {
      result <- connection.query(
        insert,
        Json
          .arr(
            name,
            priority.orNull,
            fgColor,
            bgColor,
            displayName.getJson.toString,
            isMultilang,
            isDashboard
          )
      )

      configName = insertNotNull(result).head.get[String](0)
    } yield configName

  }

  def retrieveAll()(implicit user: TableauxUser): Future[Seq[CellAnnotationConfig]] = {
    for {
      result <- connection.query(selectStatement(None))
      resultArr <- Future(resultObjectToJsonArray(result))
    } yield {
      resultArr.map(convertJsonArrayToCellAnnotationConfig)
    }
  }

  private def convertJsonArrayToCellAnnotationConfig(arr: JsonArray)(implicit
  user: TableauxUser): CellAnnotationConfig = {
    CellAnnotationConfig(
      arr.get[String](0), // name
      arr.get[Int](1), // priority
      arr.get[String](2), // fgColor
      arr.get[String](3), // bgColor
      MultiLanguageValue.fromString(arr.get[String](4)), // displayName
      arr.get[Boolean](5), // isMultilang
      arr.get[Boolean](6), // isDashboard
      arr.get[Boolean](7) // isCustom
    )
  }
}
