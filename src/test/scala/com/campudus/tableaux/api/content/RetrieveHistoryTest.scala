package com.campudus.tableaux.api.content

import com.campudus.tableaux.database._
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode

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
        _ <- dbConnection.query(
          """INSERT INTO
            |  user_table_history_1(row_id, column_id, history_type, value_type, language_type, value)
            |VALUES
            |  (1, 1, 'cell', 'numeric', 'neutral', '{"value": 42}')""".stripMargin
        )

        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
      } yield {
        val historyCell = result.getJsonArray("rows", Json.emptyArr()).getJsonObject(0)
        assertEquals(1, historyCell.getInteger("revision"))
        assertEquals(HistoryType.Cell, historyCell.getString("historyType"))
        assertEquals(NumericType.toString, historyCell.getString("valueType"))
        assertEquals(LanguageType.NEUTRAL.toString, historyCell.getString("languageType"))
        assertEquals(42, historyCell.getInteger("value"))
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
        _ <- dbConnection.query(
          """INSERT INTO
            |  user_table_history_1(row_id, column_id, history_type, value_type, language_type, value)
            |VALUES
            |  (1, 1, 'cell', 'numeric', 'neutral', null)""".stripMargin
        )

        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
      } yield {
        val historyCell = result.getJsonArray("rows", Json.emptyArr()).getJsonObject(0)
        assertEquals(Json.emptyObj(), historyCell.getJsonObject("value"))
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
        _ <- dbConnection.query(
          """INSERT INTO
            |  user_table_history_1(row_id, column_id, history_type, value_type, language_type, value)
            |VALUES
            |  (1, 1, 'cell', 'numeric', 'language', '{"value": {"de": "change1"}}'),
            |  (1, 1, 'cell', 'numeric', 'language', '{"value": {"de": "change2"}}'),
            |  (1, 1, 'cell', 'numeric', 'language', '{"value": {"de": "change3"}}')
            |  """.stripMargin
        )

        result <- sendRequest("GET", "/tables/1/columns/1/rows/1/history")
      } yield {
        val historyCells = result.getJsonArray("rows", Json.emptyArr())
        assertJSONEquals(expected, historyCells.toString, JSONCompareMode.LENIENT)
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

        // only get relevant history for lang "de"
        resultRow <- sendRequest("GET", "/tables/1/columns/1/rows/1/history/de")
          .map(_.getJsonArray("rows"))
          .map(filterByType("row"))
        resultCell <- sendRequest("GET", "/tables/1/columns/1/rows/1/history/de")
          .map(_.getJsonArray("rows"))
          .map(filterByType("cell"))
      } yield {
        assertEquals(1, resultRow.size)
        assertEquals(2, resultCell.size)
        assertJSONEquals(expected, Json.arr(resultCell: _*).toString)
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
        _ <- dbConnection.query(
          """INSERT INTO
            |  user_table_history_1(row_id, column_id, history_type, value_type, language_type, value)
            |VALUES
            |  (1, 1, 'cell', 'numeric', 'neutral', '{"value": 42}')""".stripMargin
        )
        _ <- sendRequest("GET", "/tables/1/columns/1/rows/1/history/de")
      } yield ()
    }
  }

  def createDefaultHistoryTable(dbConnection: DatabaseConnection) =
    for {
      _ <- createEmptyDefaultTable()
      res <- dbConnection.query(
        """INSERT INTO
          |  user_table_history_1
          |  (row_id, column_id, event, history_type, value_type, language_type, value, deleted_at)
          |VALUES
          |  (1, null, 'row_created',  'row',    null,     null,       null,                           null),
          |  (1, 1,    'cell_changed', 'cell',  'numeric', 'language', '{"value": {"de": "change2"}}', null),
          |  (1, 2,    'cell_changed', 'cell',  'numeric', 'language', '{"value": {"de": "change3"}}', null),
          |  (2, null, 'row_created',  'row',    null,     null,       null,                           null),
          |  (2, 1,    'cell_changed', 'cell',  'numeric', 'language', '{"value": {"de": "change5"}}', null),
          |  (2, 2,    'cell_changed', 'cell',  'numeric', 'language', '{"value": {"de": "change6"}}', null),
          |  (3, null, 'row_created',  'row',    null,     null,       null,                           now()),
          |  (3, 1,    'cell_changed', 'cell',  'numeric', 'language', '{"value": {"de": "change7"}}', now()),
          |  (3, 2,    'cell_changed', 'cell',  'numeric', 'language', '{"value": {"de": "change8"}}', now())
          |  """.stripMargin
      )
    } yield res

  @Test
  def retrieveTableHistory_includeDeletedFalse(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createDefaultHistoryTable(dbConnection)

        allRows <- sendRequest("GET", "/tables/1/history").map(_.getJsonArray("rows"))
        createdRows <- sendRequest("GET", "/tables/1/history?historyType=row").map(_.getJsonArray("rows"))
        changedCells <- sendRequest("GET", "/tables/1/history?historyType=cell").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(6, allRows.size())
        assertEquals(2, createdRows.size())
        assertEquals(4, changedCells.size())

        val deletedRowResult = createdRows.getJsonObject(0)
        assertFalse(deletedRowResult.containsKey("deletedAt"))
      }
    }
  }

  @Test
  def retrieveTableHistory_includeDeletedTrue(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createDefaultHistoryTable(dbConnection)

        allRows <- sendRequest("GET", "/tables/1/history?includeDeleted=true").map(_.getJsonArray("rows"))
        createdRows <-
          sendRequest("GET", "/tables/1/history?historyType=row&includeDeleted=true").map(_.getJsonArray("rows"))
        changedCells <-
          sendRequest("GET", "/tables/1/history?historyType=cell&includeDeleted=true").map(_.getJsonArray("rows"))
      } yield {
        assertEquals(9, allRows.size())
        assertEquals(3, createdRows.size())
        assertEquals(6, changedCells.size())

        val deletedRowResult = createdRows.getJsonObject(2)
        assertJSONEquals(
          """
            |{
            |  "revision": 7,
            |  "rowId": 3,
            |  "event": "row_created",
            |  "historyType": "row",
            |}
          """.stripMargin,
          deletedRowResult,
          JSONCompareMode.LENIENT
        )
        assertTrue(deletedRowResult.containsKey("deletedAt"))
        assertNotNull(deletedRowResult.getString("deletedAt"))
      }
    }
  }

  @Test
  def retrieveMultilanguageValuesOnlyForGivenLangtag(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createEmptyDefaultTable()

        // manually insert rows
        _ <- dbConnection.query(
          """INSERT INTO
            |  user_table_history_1
            |  (row_id, column_id, event, history_type, value_type, language_type, value)
            |VALUES
            |  (1 ,NULL ,E'row_created'        ,E'row'            ,NULL                 ,NULL        , NULL                                                                                         ),
            |  (1 ,1    ,E'cell_changed'       ,E'cell'           ,E'datetime'          ,E'language' , E'{"value": {"de-DE": "2019-01-18T00:00:00.000Z"}}'                                          ),
            |  (1 ,1    ,E'cell_changed'       ,E'cell'           ,E'datetime'          ,E'language' , E'{"value": {"en-GB": "2018-12-12T00:00:00.000Z"}}'                                          ),
            |  (1 ,1    ,E'annotation_added'   ,E'cell_flag'      ,E'needs_translation' ,E'language' , E'{"value": {"de-DE": "needs_translation"}, "uuid": "80e5bc99-72f5-45a7-a4a8-83ae49cc6409"}' ),
            |  (1 ,1    ,E'annotation_added'   ,E'cell_flag'      ,E'needs_translation' ,E'language' , E'{"value": {"en-GB": "needs_translation"}, "uuid": "8d263214-f089-43a3-b93d-5daa092804e7"}' ),
            |  (1 ,1    ,E'annotation_added'   ,E'cell_flag'      ,E'important'         ,E'neutral'  , E'{"value": "important", "uuid": "c179260c-0bcc-488a-ada4-404d16e5245c"}'                    ),
            |  (1 ,1    ,E'cell_changed'       ,E'cell'           ,E'text'              ,E'language' , E'{"value": {"en-GB": "text_change"}}'                                                       ),
            |  (1 ,1    ,E'cell_changed'       ,E'cell'           ,E'numeric'           ,E'language' , E'{"value": {"de-DE": 1}}'                                                                   ),
            |  (1 ,NULL ,E'annotation_added'   ,E'row_flag'       ,E'final'             ,E'neutral'  , E'{"value": "final"}'                                                                        ),
            |  (1 ,NULL ,E'annotation_removed' ,E'row_flag'       ,E'final'             ,E'neutral'  , E'{"value": "final"}'                                                                        ),
            |  (1 ,1    ,E'annotation_added'   ,E'cell_comment'   ,E'info'              ,E'neutral'  , E'{"value": "This is a comment", "uuid": "12f456aa-cd9e-4c95-964e-99044857714c"}'            )
            |""".stripMargin
        )

        allLangtagFiltered <- sendRequest("GET", "/tables/1/history/de-DE").map(_.getJsonArray("rows"))
        typeRow <- sendRequest("GET", "/tables/1/history/de-DE").map(_.getJsonArray("rows")).map(filterByType("row"))
        typeCell <- sendRequest("GET", "/tables/1/history/de-DE").map(_.getJsonArray("rows")).map(filterByType("cell"))
        typeCellFlag <- sendRequest("GET", "/tables/1/history/de-DE")
          .map(_.getJsonArray("rows"))
          .map(filterByType("cell_flag"))
        typeRowFlag <- sendRequest("GET", "/tables/1/history/de-DE")
          .map(_.getJsonArray("rows"))
          .map(filterByType("row_flag"))
        typeCellComment <- sendRequest("GET", "/tables/1/history/de-DE")
          .map(_.getJsonArray("rows"))
          .map(filterByType("cell_comment"))
      } yield {
        assertEquals(8, allLangtagFiltered.size())
        assertEquals(1, typeRow.size)
        assertEquals(2, typeCell.size)
        assertEquals(2, typeCellFlag.size)
        assertEquals(2, typeRowFlag.size)
        assertEquals(1, typeCellComment.size)
      }
    }
  }

  @Test
  def retrieveColumnHistory(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      for {
        _ <- createEmptyDefaultTable()

        // manually insert rows
        _ <- dbConnection.query(
          """INSERT INTO
            |  user_table_history_1
            |  (row_id, column_id, event, history_type, value_type, language_type, value)
            |VALUES
            |  (1, null, 'row_created',      'row',        null,               null,       null),
            |  (1, 1,    'cell_changed',     'cell',      'numeric',           'language', '{"value": {"de": "change2"}}'),
            |  (2, 1,    'annotation_added', 'cell_flag', 'needs_translation', 'language', '{"value":{"de":"needs_translation"}, "uuid":"9f4eef42-0476-44d8-ae10-0cdfd802a292"}'),
            |  (2, null, 'row_created',      'row',        null,               null,       null),
            |  (2, 2,    'cell_changed',     'cell',      'numeric',           'language', '{"value": {"de": "change5"}}'),
            |  (2, 2,    'annotation_added', 'cell_flag', 'needs_translation', 'language', '{"value":{"de":"needs_translation"}, "uuid":"e3d371a4-e3cc-4697-83fe-ef32bcddd3e6"}')
            |  """.stripMargin
        )

        allRows <- sendRequest("GET", "/tables/1/history").map(_.getJsonArray("rows"))
        createdRows <- sendRequest("GET", "/tables/1/columns/1/history?historyType=row").map(_.getJsonArray("rows"))
        columnHistory <- sendRequest("GET", "/tables/1/columns/1/history").map(_.getJsonArray("rows"))
        changedCells <- sendRequest("GET", "/tables/1/columns/1/history?historyType=cell")
          .map(_.getJsonArray("rows"))
          .map(filterByType("cell"))
        addedAnnotations <- sendRequest("GET", "/tables/1/columns/1/history?historyType=cell_flag")
          .map(_.getJsonArray("rows"))
          .map(filterByType("cell_flag"))

      } yield {
        assertEquals(6, allRows.size())
        // historyType=row makes no sense for columns endpoint, so it should return empty
        assertEquals(0, createdRows.size())
        assertEquals(2, columnHistory.size())
        assertEquals(1, changedCells.size)
        assertEquals(1, addedAnnotations.size)

        val expectedChangedCells =
          """
            |[{
            |  "revision": 2, "rowId": 1, "columnId": 1, "event": "cell_changed", "historyType": "cell",
            |  "value": {"de": "change2"}
            |}]
        """.stripMargin

        assertJSONEquals(expectedChangedCells, Json.arr(changedCells: _*).toString)
        val expectedAddedAnnotations =
          """
            |[{
            |  "revision": 3, "rowId": 2, "columnId": 1, "event": "annotation_added", "historyType": "cell_flag",
            |  "value": {"de": "needs_translation"}, "uuid": "9f4eef42-0476-44d8-ae10-0cdfd802a292"
            |}]
            |""".stripMargin
        assertJSONEquals(expectedAddedAnnotations, Json.arr(addedAnnotations: _*).toString)
      }
    }
  }

  def filterByType(historyType: String)(jsonArray: JsonArray): Seq[JsonObject] = {
    import scala.collection.JavaConverters._

    jsonArray.asScala
      .collect({
        case obj: JsonObject => obj
      })
      .toSeq
      .filter(_.getString("historyType") == historyType)
  }
}
