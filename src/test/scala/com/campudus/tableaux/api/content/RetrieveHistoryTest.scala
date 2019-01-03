package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.{DatabaseConnection, LanguageType, NumericType}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class RetrieveHistoryTest extends TableauxTestBase {

  @Test
  def retrieveSimpleValue(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createEmptyDefaultTable()

        // manually insert row
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1(row_id, column_id, type, language_type, value)
                                  |VALUES
                                  |  (1, 1, 'numeric', 'neutral', '{"value": 42}')""".stripMargin)

        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
      } yield {
        val historyCell = result.getJsonArray("rows", Json.emptyArr()).getJsonObject(0)
        assertEquals(historyCell.getInteger("revision"), 1)
        assertEquals(historyCell.getString("type"), NumericType.toString)
        assertEquals(historyCell.getString("languageType"), LanguageType.NEUTRAL.toString)
        assertEquals(historyCell.getInteger("value"), 42)
      }
    }
  }

  @Test
  def retrieveInvalidValueJson(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createEmptyDefaultTable()

        // manually insert row
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1(row_id, column_id, type, language_type, value)
                                  |VALUES
                                  |  (1, 1, 'numeric', 'neutral', null)""".stripMargin)

        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
      } yield {
        val historyCell = result.getJsonArray("rows", Json.emptyArr()).getJsonObject(0)
        assertEquals(historyCell.getJsonObject("value"), Json.emptyObj())
      }
    }
  }

  @Test
  def retrieveMultilanguageValues(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      val expected =
        """
          |[{
          |  "value": {"de": "change1"}
          |}, {
          |  "value": {"de": "change2"}
          |}, {
          |  "value": {"de": "change3"}
          |}]
        """.stripMargin

      for {
        _ <- createEmptyDefaultTable()

        // manually insert rows
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1(row_id, column_id, type, language_type, value)
                                  |VALUES
                                  |  (1, 1, 'numeric', 'language', '{"value": {"de": "change1"}}'),
                                  |  (1, 1, 'numeric', 'language', '{"value": {"de": "change2"}}'),
                                  |  (1, 1, 'numeric', 'language', '{"value": {"de": "change3"}}')
                                  |  """.stripMargin)

        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
      } yield {
        val historyCells = result.getJsonArray("rows", Json.emptyArr())
        assertEqualsJSON(expected, historyCells.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def retrieveMultilanguageValuesFiltered(implicit c: TestContext): Unit = {
    okTest {
      val expected =
        """
          |[{
          |  "value": {"de": "de change2"}
          |}, {
          |  "value": {"de": "de change3"}
          |}]
        """.stripMargin

      for {
        _ <- createTableWithMultilanguageColumns("history test")
        _ <- sendRequest("POST", "/tables/1/rows")
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", Json.obj("value" -> Json.obj("en" -> "en change1")))
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", Json.obj("value" -> Json.obj("de" -> "de change2")))
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", Json.obj("value" -> Json.obj("de" -> "de change3")))
        _ <- sendRequest("POST", "/tables/1/columns/1/rows/1", Json.obj("value" -> Json.obj("en" -> "en change4")))

        // only get history for lang "de"
        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history/de")
      } yield {
        val historyCells = result.getJsonArray("rows", Json.emptyArr())

        assertEqualsJSON(expected, historyCells.toString, JSONCompareMode.LENIENT)
      }
    }
  }

  @Test
  def retrieveMultilanguageValuesFromSingleLanguageColumn(implicit c: TestContext): Unit = {
    exceptionTest("error.request.invalid") {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createEmptyDefaultTable()

        // manually insert row
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1(row_id, column_id, type, language_type, value)
                                  |VALUES
                                  |  (1, 1, 'numeric', 'neutral', '{"value": 42}')""".stripMargin)
        _ <- sendRequest("GET", "/tables/1/columns/1/rows/1/history/de")
      } yield ()
    }
  }

  @Test
  def retrieveTableHistory(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createEmptyDefaultTable()

        // manually insert rows
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1
                                  |  (row_id, column_id, event, type, language_type, value)
                                  |VALUES
                                  |  (1, null, 'row_created',     null,     null,       null),
                                  |  (1, 2,    'cell_changed',   'numeric', 'language', '{"value": {"de": "change2"}}'),
                                  |  (1, 3,    'cell_changed',   'numeric', 'language', '{"value": {"de": "change3"}}'),
                                  |  (2, null, 'row_created',     null,     null,       null),
                                  |  (2, 2,    'cell_changed',   'numeric', 'language', '{"value": {"de": "change5"}}'),
                                  |  (2, 3,    'cell_changed',   'numeric', 'language', '{"value": {"de": "change6"}}')
                                  |  """.stripMargin)

        allRows <- sendRequest("GET", "/tables/1/history").map(_.getJsonArray("rows"))
        createdRows <- sendRequest("GET", "/tables/1/history?event=row_created").map(_.getJsonArray("rows"))
        changedCells <- sendRequest("GET", "/tables/1/history?event=cell_changed").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(6, allRows.size())
        assertEquals(2, createdRows.size())
        assertEquals(4, changedCells.size())
      }
    }
  }
}
