package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.domain.DomainObject
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json._

import scala.concurrent.Future

import java.net.URLEncoder
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class RetrieveGroupColumnTest extends TableauxTestBase {

  def createTextColumnJson(name: String) = Columns(TextCol(name)).getJson

  def createBooleanColumnJson(name: String) = Columns(BooleanCol(name)).getJson

  def createGroupColumnJson(name: String, groups: Seq[ColumnId]) = Columns(GroupCol(name, groups)).getJson

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

      groupColumnId = groupColumn.getLong("id")

      rows <- sendRequest("GET", s"/tables/$tableId/rows")
        .map(_.getJsonArray("rows"))

      rowsOnlyGroupColumn <- sendRequest("GET", s"/tables/$tableId/columns/$groupColumnId/rows")
        .map(_.getJsonArray("rows"))
    } yield {
      import scala.collection.JavaConverters._

      val expected = Json.arr(
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj(
                "de-DE" -> s"Hallo, $tableName Welt!",
                "en-GB" -> s"Hello, $tableName World!"
              ),
              Json.obj("de-DE" -> true),
              Json.obj("de-DE" -> 3.1415926),
              Json.obj("en-GB" -> s"Hello, $tableName Col 1 Row 1!"),
              Json.obj("en-GB" -> s"Hello, $tableName Col 2 Row 1!"),
              Json.obj("de-DE" -> "2015-01-01"),
              Json.obj("de-DE" -> "2015-01-01T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt!",
                  "en-GB" -> s"Hello, $tableName World!"
                ),
                Json.obj("de-DE" -> true),
                Json.obj("de-DE" -> 3.1415926),
                Json.obj("en-GB" -> s"Hello, $tableName Col 1 Row 1!"),
                Json.obj("en-GB" -> s"Hello, $tableName Col 2 Row 1!"),
                Json.obj("de-DE" -> "2015-01-01"),
                Json.obj("de-DE" -> "2015-01-01T13:37:47.110Z")
              )
            )
        ),
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj(
                "de-DE" -> s"Hallo, $tableName Welt2!",
                "en-GB" -> s"Hello, $tableName World2!"
              ),
              Json.obj("de-DE" -> false),
              Json.obj("de-DE" -> 2.1415926),
              Json.obj("en-GB" -> s"Hello, $tableName Col 1 Row 2!"),
              Json.obj("en-GB" -> s"Hello, $tableName Col 2 Row 2!"),
              Json.obj("de-DE" -> "2015-01-02"),
              Json.obj("de-DE" -> "2015-01-02T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt2!",
                  "en-GB" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de-DE" -> false),
                Json.obj("de-DE" -> 2.1415926),
                Json.obj("en-GB" -> s"Hello, $tableName Col 1 Row 2!"),
                Json.obj("en-GB" -> s"Hello, $tableName Col 2 Row 2!"),
                Json.obj("de-DE" -> "2015-01-02"),
                Json.obj("de-DE" -> "2015-01-02T13:37:47.110Z")
              )
            )
        )
      )

      val expectedOnlyGroupColumn = expected.asScala
        .map(_.asInstanceOf[JsonObject].getJsonArray("values").asScala.last.asInstanceOf[JsonArray])
        .map(groupCell => Json.obj("values" -> Json.arr(groupCell)))
        .toSeq

      assertJSONEquals(expected, rows)
      assertJSONEquals(Json.arr(expectedOnlyGroupColumn: _*), rowsOnlyGroupColumn)
    }
  }

  @Test
  def retrieveComplexTableWithOneGroupColumnFilteredByIds(implicit c: TestContext): Unit = okTest {
    val tableName = "test"
    for {
      (tableId, columnIds, _) <- createFullTableWithMultilanguageColumns(tableName)

      groupColumn <- sendRequest("POST", "/tables/1/columns", createGroupColumnJson("groupcolumn", Seq(1, 2, 3)))
        .map(_.getJsonArray("columns").getJsonObject(0))

      groupColumnId = groupColumn.getLong("id")

      rowsOnlyGroupColumnValue <- sendRequest("GET", s"/tables/$tableId/rows?columnIds=$groupColumnId")
        .map(_.getJsonArray("rows"))

      groupColumnIdWithOthers = s"$groupColumnId,6,7";

      rowsGroupColumnWithOthers <- sendRequest("GET", s"/tables/$tableId/rows?columnIds=$groupColumnIdWithOthers")
        .map(_.getJsonArray("rows"))

    } yield {
      import scala.collection.JavaConverters._

      val expectedOnlyGroupColumnValue = Json.arr(
        Json.obj(
          "values" ->
            Json.arr(
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt!",
                  "en-GB" -> s"Hello, $tableName World!"
                ),
                Json.obj("de-DE" -> true),
                Json.obj("de-DE" -> 3.1415926)
              )
            )
        ),
        Json.obj(
          "values" ->
            Json.arr(
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt2!",
                  "en-GB" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de-DE" -> false),
                Json.obj("de-DE" -> 2.1415926)
              )
            )
        )
      )
      val expectedGroupColumnWithOthers = Json.arr(
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj("de-DE" -> "2015-01-01"),
              Json.obj("de-DE" -> "2015-01-01T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt!",
                  "en-GB" -> s"Hello, $tableName World!"
                ),
                Json.obj("de-DE" -> true),
                Json.obj("de-DE" -> 3.1415926)
              )
            )
        ),
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj("de-DE" -> "2015-01-02"),
              Json.obj("de-DE" -> "2015-01-02T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt2!",
                  "en-GB" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de-DE" -> false),
                Json.obj("de-DE" -> 2.1415926)
              )
            )
        )
      )

      assertJSONEquals(expectedOnlyGroupColumnValue, rowsOnlyGroupColumnValue)
      assertJSONEquals(expectedGroupColumnWithOthers, rowsGroupColumnWithOthers)
    }
  }

  @Test
  def retrieveComplexTableWithOneGroupColumnFilteredByNames(implicit c: TestContext): Unit = okTest {
    val tableName = "test"
    for {
      (tableId, columnIds, _) <- createFullTableWithMultilanguageColumns(tableName)

      groupColumn <- sendRequest("POST", "/tables/1/columns", createGroupColumnJson("groupcolumn", Seq(1, 2, 3)))
        .map(_.getJsonArray("columns").getJsonObject(0))

      groupColumnName = groupColumn.getString("name")

      rowsOnlyGroupColumnValue <- sendRequest("GET", s"/tables/$tableId/rows?columnNames=$groupColumnName")
        .map(_.getJsonArray("rows"))

      groupColumnNameWithOthers = URLEncoder.encode(s"$groupColumnName,Test Column 6,Test Column 7", "UTF-8");

      rowsGroupColumnWithOther <- sendRequest("GET", s"/tables/$tableId/rows?columnNames=$groupColumnNameWithOthers")
        .map(_.getJsonArray("rows"))

    } yield {
      import scala.collection.JavaConverters._

      val expectedOnlyGroupColumnValue = Json.arr(
        Json.obj(
          "values" ->
            Json.arr(
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt!",
                  "en-GB" -> s"Hello, $tableName World!"
                ),
                Json.obj("de-DE" -> true),
                Json.obj("de-DE" -> 3.1415926)
              )
            )
        ),
        Json.obj(
          "values" ->
            Json.arr(
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt2!",
                  "en-GB" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de-DE" -> false),
                Json.obj("de-DE" -> 2.1415926)
              )
            )
        )
      )
      val expectedGroupColumnWithOther = Json.arr(
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj("de-DE" -> "2015-01-01"),
              Json.obj("de-DE" -> "2015-01-01T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt!",
                  "en-GB" -> s"Hello, $tableName World!"
                ),
                Json.obj("de-DE" -> true),
                Json.obj("de-DE" -> 3.1415926)
              )
            )
        ),
        Json.obj(
          "values" ->
            Json.arr(
              Json.obj("de-DE" -> "2015-01-02"),
              Json.obj("de-DE" -> "2015-01-02T13:37:47.110Z"),
              Json.arr(
                Json.obj(
                  "de-DE" -> s"Hallo, $tableName Welt2!",
                  "en-GB" -> s"Hello, $tableName World2!"
                ),
                Json.obj("de-DE" -> false),
                Json.obj("de-DE" -> 2.1415926)
              )
            )
        )
      )

      assertJSONEquals(expectedOnlyGroupColumnValue, rowsOnlyGroupColumnValue)
      assertJSONEquals(expectedGroupColumnWithOther, rowsGroupColumnWithOther)
    }
  }

  @Test
  def retrieveGroupColumnAndChangeGroupedValue(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest("POST", "/tables", Json.obj("name" -> "table1"))

      textColumnId <- sendCreateColumnRequest(1, createTextColumnJson("textcolumn"))
      booleanColumnId <- sendCreateColumnRequest(1, createBooleanColumnJson("booleancolumn"))

      textColumn <- sendRequest("GET", s"/tables/1/columns/$textColumnId")
      booleanColumn <- sendRequest("GET", s"/tables/1/columns/$booleanColumnId")

      _ <- sendRequest(
        "POST",
        "/tables/1/columns",
        createGroupColumnJson("groupcolumn", Seq(textColumnId, booleanColumnId))
      )
        .map(_.getJsonArray("columns").getJsonObject(0))

      createRow = Rows(
        Json.arr(textColumn, booleanColumn),
        Json.obj(
          "textcolumn" -> "blub",
          "booleancolumn" -> true
        )
      )

      createdRow <- sendRequest("POST", "/tables/1/rows", createRow)
        .map(_.getJsonArray("rows").getJsonObject(0))

      rowId = createdRow.getInteger("id")

      retrievedRow <- sendRequest("GET", s"/tables/1/rows/$rowId")

      _ <- sendRequest("POST", s"/tables/1/columns/$textColumnId/rows/$rowId", Json.obj("value" -> "test"))

      changedRow <- sendRequest("GET", s"/tables/1/rows/$rowId")
    } yield {
      assertEquals(Json.arr("blub", true, Json.arr("blub", true)), createdRow.getJsonArray("values"))
      assertEquals(Json.arr("blub", true, Json.arr("blub", true)), retrievedRow.getJsonArray("values"))

      assertEquals(Json.arr("test", true, Json.arr("test", true)), changedRow.getJsonArray("values"))
    }
  }

  @Test
  def retrieveLinkedGroupColumnAndChangeGroupedValue(implicit c: TestContext): Unit = okTest {
    for {
      // create two tables which will be linked
      table1 <- sendRequest("POST", "/tables", Json.obj("name" -> "test1")).map(_.getLong("id"))
      table2 <- sendRequest("POST", "/tables", Json.obj("name" -> "test2")).map(_.getLong("id"))

      // create columns for table 1
      textColumnId <- sendCreateColumnRequest(table1, Columns(TextCol("textcolumn")))
      booleanColumnId <- sendCreateColumnRequest(table1, Columns(BooleanCol("booleancolumn")))
      _ <- sendRequest(
        "POST",
        s"/tables/$table1/columns",
        Columns(Identifier(GroupCol("groupcolumn", Seq(textColumnId, booleanColumnId))))
      )

      textColumn <- sendRequest("GET", s"/tables/$table1/columns/$textColumnId")
      booleanColumn <- sendRequest("GET", s"/tables/$table1/columns/$booleanColumnId")

      columns <- sendRequest("GET", s"/tables/$table1/columns")
        .map(_.getJsonArray("columns"))

      _ = assert(columns.size() == 3)

      // create row for table 1
      createRow11 = Rows(Json.arr(textColumn, booleanColumn), Json.obj("textcolumn" -> "blub", "booleancolumn" -> true))

      rowId11 <- sendRequest("POST", s"/tables/$table1/rows", createRow11)
        .map(_.getJsonArray("rows").getJsonObject(0).getInteger("id"))

      // create columns for table 2
      _ <- sendCreateColumnRequest(table2, Columns(Identifier(TextCol("textcolumn"))))
      linkColumnId <- sendCreateColumnRequest(table2, Columns(LinkUniDirectionalCol("linkcolumn", table1)))

      columns <- sendRequest("GET", s"/tables/$table2/columns")
        .map(_.getJsonArray("columns"))

      _ = assert(columns.size() == 2)

      linkColumn <- sendRequest("GET", s"/tables/$table2/columns/$linkColumnId")

      // create row for table 2
      createRow21 = Rows(Json.arr(linkColumn), Json.obj("linkcolumn" -> rowId11))

      rowId21 <- sendRequest("POST", s"/tables/$table2/rows", createRow21)
        .map(_.getJsonArray("rows").getJsonObject(0).getInteger("id"))

      retrievedRow <- sendRequest("GET", s"/tables/$table2/rows/$rowId21")

      _ <- sendRequest("POST", s"/tables/1/columns/$textColumnId/rows/$rowId11", Json.obj("value" -> "test"))

      changedRow <- sendRequest("GET", s"/tables/$table2/rows/$rowId21")
    } yield {

      def expectedLinkCell(s: String) = Json.arr(
        Json.obj(
          "id" -> rowId11,
          "value" -> Json.arr(s, true)
        )
      )

      assertEquals(Json.arr(null, expectedLinkCell("blub")), retrievedRow.getJsonArray("values"))
      assertEquals(Json.arr(null, expectedLinkCell("test")), changedRow.getJsonArray("values"))
    }
  }
}
