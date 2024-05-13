package com.campudus.tableaux.api.permission

import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.model.SystemModel
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import com.campudus.tableaux.testtools.RequestCreation.{Identifier, TextCol}

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class CreateRowWithRowPermissionsTest extends TableauxTestBase {

  val createTableJson = Json.obj("name" -> "Test Nr. 1")

  def createTextColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.TextCol(name)).getJson

  def createNumberColumnJson(name: String) = RequestCreation.Columns().add(RequestCreation.NumericCol(name)).getJson

  @Test
  def createMultipleFullRowsWithRowPermissions(implicit c: TestContext): Unit = {
    okTest {
      val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
      val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

      val rowPermissions = Json.arr("perm_group_1", "perm_group_2")

      val valuesRow = Json.obj(
        "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
        "rows" -> Json.arr(
          Json.obj("values" -> Json.arr("Test Field 1", 2)),
          Json.obj("values" -> Json.arr("Test Field 2", 5))
        ),
        "rowPermissions" -> rowPermissions
      )
      val expectedJson = Json.obj(
        "status" -> "ok",
        "rows" -> Json.arr(
          Json.obj("id" -> 1, "values" -> Json.arr("Test Field 1", 2)),
          Json.obj("id" -> 2, "values" -> Json.arr("Test Field 2", 5))
        )
      )

      for {
        _ <- sendRequest("POST", "/tables", createTableJson)
        _ <- sendRequest("POST", "/tables/1/columns", createTextColumnJson("Test Column 1"))
        _ <- sendRequest("POST", "/tables/1/columns", createNumberColumnJson("Test Column 2"))
        test <- sendRequest("POST", "/tables/1/rows", valuesRow)

        rowPermissionsResult <-
          dbConnection.query("SELECT row_permissions FROM user_table_1")
            .map(_.getJsonArray("results").getJsonArray(0).getString(0))

        rowPermissionsHistoryResult <-
          dbConnection.query(
            s"""
               |SELECT value FROM user_table_history_1
               |WHERE event = 'row_permissions_changed'
               |AND history_type = 'row_permissions'""".stripMargin
          ).map(_.getJsonArray("results"))
        rowPermissionsHistoryValue1 = Json.fromObjectString(rowPermissionsHistoryResult.getJsonArray(0).getString(0))
        rowPermissionsHistoryValue2 = Json.fromObjectString(rowPermissionsHistoryResult.getJsonArray(1).getString(0))
      } yield {
        assertJSONEquals(expectedJson, test)
        assertJSONEquals(rowPermissions, Json.fromArrayString(rowPermissionsResult))
        assertEquals(2, rowPermissionsHistoryResult.size())
        assertJSONEquals(rowPermissions, rowPermissionsHistoryValue1.getJsonArray("value"))
        assertJSONEquals(rowPermissions, rowPermissionsHistoryValue2.getJsonArray("value"))
      }
    }
  }
}
