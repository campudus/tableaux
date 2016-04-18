package com.campudus.tableaux.api.system

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.JsonArray

@RunWith(classOf[VertxUnitRunner])
class DatabaseVersioningTest extends TableauxTestBase {

  @Test
  def checkVersion3(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)
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
  def checkUpdateOfOrdering(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(verticle, databaseConfig)
    val dbConnection = DatabaseConnection(verticle, sqlConnection)
    val system = SystemModel(dbConnection)

    for {
      _ <- system.uninstall()
      _ <- system.install(Some(1))

      // Needs to create tables like it did in database version 1
      inserted <- dbConnection.query("INSERT INTO system_table (user_table_name) VALUES ('table1'),('table2') RETURNING table_id")
      tableId1 = inserted.getJsonArray("results").getJsonArray(0).getLong(0)
      _ <- dbConnection.query(s"CREATE TABLE user_table_$tableId1 (id BIGSERIAL, PRIMARY KEY (id))")
      _ <- dbConnection.query(s"CREATE SEQUENCE system_columns_column_id_table_$tableId1")
      tableId2 = inserted.getJsonArray("results").getJsonArray(1).getLong(0)
      _ <- dbConnection.query(s"CREATE TABLE user_table_$tableId2 (id BIGSERIAL, PRIMARY KEY (id))")
      _ <- dbConnection.query(s"CREATE SEQUENCE system_columns_column_id_table_$tableId2")

      _ <- system.update()
      tablesOrdering <- dbConnection.query("SELECT table_id, ordering FROM system_table")
    } yield {

      assertTrue(tablesOrdering.getJsonArray("results").size() > 0)

      import scala.collection.JavaConverters._
      for {
        (tableId, ordering) <- tablesOrdering.getJsonArray("results").asScala
          .map(_.asInstanceOf[JsonArray])
          .map(elem => (elem.getLong(0), elem.getLong(1)))
      } yield {
        assertEquals(tableId, ordering)
      }
    }
  }
}
