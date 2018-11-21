package com.campudus.tableaux.api.content

import com.campudus.tableaux.database.model.RetrieveHistoryModel
import com.campudus.tableaux.database.{DatabaseConnection, LanguageType, NumericType}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

@RunWith(classOf[VertxUnitRunner])
class RetrieveHistoryTest extends TableauxTestBase {

  val createTableJson: JsonObject = Json.obj("name" -> "History Test Table 1")

  @Test
  def retrieveSimple(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
      val historyModel = RetrieveHistoryModel(dbConnection)

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        // manually insert row
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1(row_id, column_id, column_type, multilanguage, value)
                                  |VALUES
                                  |  (1, 1, 'numeric', 'neutral', '{"value": 42}')""".stripMargin)

        result <- historyModel.retrieve(1, 1, 1)
      } yield {
        val historyCell = result.rows.head
        assertEquals(historyCell.revision, 1)
        assertEquals(historyCell.columnType, NumericType)
        assertEquals(historyCell.languageType.toString, LanguageType.NEUTRAL.toString)
        assertEquals(historyCell.value, Json.obj("value" -> 42))
      }
    }
  }

  @Test
  def retrieveInvalidValueJson(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
      val historyModel = RetrieveHistoryModel(dbConnection)

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)

        // manually insert row
        _ <- dbConnection.query("""INSERT INTO
                                  |  user_table_history_1(row_id, column_id, column_type, multilanguage, value)
                                  |VALUES
                                  |  (1, 1, 'numeric', 'neutral', null)""".stripMargin)

        result <- historyModel.retrieve(1, 1, 1)
      } yield {
        val historyCell = result.rows.head
        assertEquals(historyCell.value, Json.emptyObj())
      }
    }
  }
}
