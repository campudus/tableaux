package com.campudus.tableaux.api.system

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.router.SystemRouter
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonArray}

@RunWith(classOf[VertxUnitRunner])
class DatabaseVersioningTest extends TableauxTestBase {

  private def sqlUserTable(id: TableId) = s"CREATE TABLE user_table_$id (id BIGSERIAL, PRIMARY KEY (id))"

  private def sqlUserLangTable(id: TableId) =
    s"""
       |CREATE TABLE user_table_lang_$id (
       |  id BIGINT,
       |  langtag VARCHAR(255),
       |
       |  PRIMARY KEY (id, langtag),
       |
       |  FOREIGN KEY(id)
       |  REFERENCES user_table_$id(id)
       |  ON DELETE CASCADE
       |)""".stripMargin

  private def sqlColumnSequence(id: TableId) = s"CREATE SEQUENCE system_columns_column_id_table_$id"

  @Test
  def checkVersion3(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(this, databaseConfig)
    val dbConnection = DatabaseConnection(this, sqlConnection)
    val system = SystemModel(dbConnection)

    for {
      _ <- system.uninstall()
      _ <- system.install(Some(1))
      result1 <- dbConnection.query("SELECT * FROM system_table")
      _ <- system.uninstall()
      _ <- system.install(Some(3))
      result2 <- dbConnection.query("SELECT * FROM system_table")
    } yield {
      assertTrue(!result1.getJsonArray("fields").contains("is_hidden"))
      assertTrue(result2.getJsonArray("fields").contains("is_hidden"))
    }
  }

  @Test
  def checkUpdateOfOrdering(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this, databaseConfig)
      val dbConnection = DatabaseConnection(this, sqlConnection)
      val system = SystemModel(dbConnection)

      for {
        _ <- system.uninstall()
        _ <- system.install(Some(1))

        // Needs to create tables like it did in database version 1
        inserted <- dbConnection.query(
          "INSERT INTO system_table (user_table_name) VALUES ('table1'),('table2') RETURNING table_id")

        tableId1 = inserted.getJsonArray("results").getJsonArray(0).getLong(0)
        _ <- dbConnection.query(sqlUserTable(tableId1))
        _ <- dbConnection.query(sqlUserLangTable(tableId1))
        _ <- dbConnection.query(sqlColumnSequence(tableId1))

        tableId2 = inserted.getJsonArray("results").getJsonArray(1).getLong(0)
        _ <- dbConnection.query(sqlUserTable(tableId2))
        _ <- dbConnection.query(sqlUserLangTable(tableId2))
        _ <- dbConnection.query(sqlColumnSequence(tableId2))

        _ <- system.update()
        tablesOrdering <- dbConnection.query("SELECT table_id, ordering FROM system_table")
      } yield {

        assertTrue(tablesOrdering.getJsonArray("results").size() > 0)

        import scala.collection.JavaConverters._
        for {
          (tableId, ordering) <- tablesOrdering
            .getJsonArray("results")
            .asScala
            .map(_.asInstanceOf[JsonArray])
            .map(elem => (elem.getLong(0), elem.getLong(1)))
        } yield {
          assertEquals(tableId, ordering)
        }
      }
    }
  }

  @Test
  def checkUpdateOverHttp(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this, databaseConfig)
      val dbConnection = DatabaseConnection(this, sqlConnection)
      val system = SystemModel(dbConnection)

      val nonce = SystemRouter.generateNonce()

      for {
        _ <- system.uninstall()
        _ <- system.install(Some(1))

        // Needs to create tables like it did in database version 1
        inserted <- dbConnection.query(
          "INSERT INTO system_table (user_table_name) VALUES ('table1'),('table2') RETURNING table_id")

        tableId1 = inserted.getJsonArray("results").getJsonArray(0).getLong(0)
        _ <- dbConnection.query(sqlUserTable(tableId1))
        _ <- dbConnection.query(sqlUserLangTable(tableId1))
        _ <- dbConnection.query(sqlColumnSequence(tableId1))

        tableId2 = inserted.getJsonArray("results").getJsonArray(1).getLong(0)
        _ <- dbConnection.query(sqlUserTable(tableId2))
        _ <- dbConnection.query(sqlUserLangTable(tableId2))
        _ <- dbConnection.query(sqlColumnSequence(tableId2))

        updatePost <- sendRequest("POST", s"/system/update?nonce=$nonce")
      } yield {
        assertContains(Json.obj("status" -> "ok"), updatePost)
      }
    }
  }
}
