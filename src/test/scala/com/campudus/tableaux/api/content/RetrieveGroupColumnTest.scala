package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json._

import scala.concurrent.Future

@RunWith(classOf[VertxUnitRunner])
class RetrieveGroupColumnTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "table1")

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson

  def createBooleanColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.BooleanCol(name)).getJson

  def createGroupColumnJson(name: String, groups: Seq[ColumnId]) =
    RequestCreation.Columns().add(RequestCreation.GroupCol(name, groups)).getJson

  def sendCreateColumnRequest(tableId: TableId, columnsJson: DomainObject): Future[Int] =
    sendCreateColumnRequest(tableId, columnsJson.getJson)

  def sendCreateColumnRequest(tableId: TableId, columnsJson: JsonObject): Future[Int] =
    sendRequest("POST", s"/tables/$tableId/columns", columnsJson)
      .map(_.getJsonArray("columns").getJsonObject(0).getInteger("id"))

  @Test
  def retrieveComplexTableWithOneGroupColumn(implicit c: TestContext): Unit = okTest {
    val tableName = "test"
    for {
      (tableId, columnIds, _) <- createFullTableWithMultilanguageColumns(tableName)

      groupColumn <- sendRequest("POST", "/tables/1/columns", createGroupColumnJson("groupcolumn", columnIds))
        .map(_.getJsonArray("columns").getJsonObject(0))

      rows <- sendRequest("GET", s"/tables/$tableId/rows")
        .map(_.getJsonArray("rows"))

      rowsOnlyGroupColumn <- sendRequest("GET", s"/tables/$tableId/columns/${groupColumn.getLong("id")}/rows")
        .map(_.getJsonArray("rows"))
    } yield {
      import scala.collection.JavaConverters._

      val excepted = Json.arr(
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj(
                "de_DE" -> s"Hallo, $tableName Welt!",
                "en_US" -> s"Hello, $tableName World!"
              ),
              Json.obj("de_DE" -> true),
              Json.obj("de_DE" -> 3.1415926),
              Json.obj("en_US" -> s"Hello, $tableName Col 1 Row 1!"),
              Json.obj("en_US" -> s"Hello, $tableName Col 2 Row 1!"),
              Json.obj("de_DE" -> "2015-01-01"),
              Json.obj("de_DE" -> "2015-01-01T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de_DE" -> s"Hallo, $tableName Welt!",
                  "en_US" -> s"Hello, $tableName World!"
                ),
                Json.obj("de_DE" -> true),
                Json.obj("de_DE" -> 3.1415926),
                Json.obj("en_US" -> s"Hello, $tableName Col 1 Row 1!"),
                Json.obj("en_US" -> s"Hello, $tableName Col 2 Row 1!"),
                Json.obj("de_DE" -> "2015-01-01"),
                Json.obj("de_DE" -> "2015-01-01T13:37:47.110Z")
              )
            )
        ),
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj(
                "de_DE" -> s"Hallo, $tableName Welt2!",
                "en_US" -> s"Hello, $tableName World2!"
              ),
              Json.obj("de_DE" -> false),
              Json.obj("de_DE" -> 2.1415926),
              Json.obj("en_US" -> s"Hello, $tableName Col 1 Row 2!"),
              Json.obj("en_US" -> s"Hello, $tableName Col 2 Row 2!"),
              Json.obj("de_DE" -> "2015-01-02"),
              Json.obj("de_DE" -> "2015-01-02T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de_DE" -> s"Hallo, $tableName Welt2!",
                  "en_US" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de_DE" -> false),
                Json.obj("de_DE" -> 2.1415926),
                Json.obj("en_US" -> s"Hello, $tableName Col 1 Row 2!"),
                Json.obj("en_US" -> s"Hello, $tableName Col 2 Row 2!"),
                Json.obj("de_DE" -> "2015-01-02"),
                Json.obj("de_DE" -> "2015-01-02T13:37:47.110Z")
              )
            )
        )
      )

      val exceptedOnlyGroupColumn = excepted.asScala
        .map(_.asInstanceOf[JsonObject].getJsonArray("values").asScala.last.asInstanceOf[JsonArray])
        .map(groupCell => Json.obj("values" -> Json.arr(groupCell)))
        .toSeq

      assertContainsDeep(excepted, rows)
      assertContainsDeep(Json.arr(exceptedOnlyGroupColumn: _*), rowsOnlyGroupColumn)
    }
  }
}
