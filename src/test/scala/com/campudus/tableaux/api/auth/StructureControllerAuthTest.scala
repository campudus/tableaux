package com.campudus.tableaux.api.auth

import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{DisplayInfos, GenericTable}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class StructureControllerAuthTest extends TableauxTestBase {

  def createStructureController(roleModel: RoleModel = RoleModel(Json.emptyObj())): StructureController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model, roleModel)
  }

  @Test
  def deleteTable_authorized_ok(implicit c: TestContext): Unit = okTest {

    setRequestRoles("delete-tables")

    val roleModel = RoleModel("""
                                |{
                                |  "delete-tables": [
                                |    {
                                |      "type": "grant",
                                |      "action": ["delete"],
                                |      "scope": "table"
                                |    }
                                |  ]
                                |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {

      table <- controller.createTable("TestTable",
                                      hidden = false,
                                      langtags = None,
                                      displayInfos = DisplayInfos.fromJson(Json.emptyObj()),
                                      tableType = GenericTable,
                                      tableGroupId = None)

      _ <- controller.deleteTable(table.id)
    } yield ()
  }

  @Test
  def deleteTable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val roleModel = RoleModel("""
                                  |{
                                  |  "delete-tables": [
                                  |    {
                                  |      "type": "grant",
                                  |      "action": ["delete"],
                                  |      "scope": "table"
                                  |    }
                                  |  ]
                                  |}""".stripMargin)

      val controller = createStructureController(roleModel)

      for {
        table <- controller.createTable("TestTable",
                                        hidden = false,
                                        langtags = None,
                                        displayInfos = DisplayInfos.fromJson(Json.emptyObj()),
                                        tableType = GenericTable,
                                        tableGroupId = None)

        _ <- controller.deleteTable(table.id)
      } yield ()
    }

  @Test
  def retrieveTables_threeTablesAllViewable_returnAll(implicit c: TestContext): Unit = okTest {

    setRequestRoles("view-tables")

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      _ <- createDefaultTable("Test1")
      _ <- createDefaultTable("Test2")
      _ <- createDefaultTable("Test3")

      tables <- controller.retrieveTables.map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(3, tables.size())
    }
  }

  @Test
  def retrieveTables_threeTablesTwoViewable_returnTwo(implicit c: TestContext): Unit = okTest {

    setRequestRoles("view-tables")

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "id": "1|3"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      _ <- createDefaultTable("Test1")
      _ <- createDefaultTable("Test2") // not viewable
      _ <- createDefaultTable("Test3")

      tables <- controller.retrieveTables.map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(2, tables.size())

      val tableIds = asSeqOf[JsonObject](tables).map(_.getInteger("id"))
      assertEquals(Seq(1, 3), tableIds)
    }
  }

  @Test
  def retrieveTables_noViewPermission_returnEmptyList(implicit c: TestContext): Unit = okTest {

    val controller = createStructureController()

    for {
      _ <- createDefaultTable("Test1")
      _ <- createDefaultTable("Test2") // not viewable
      _ <- createDefaultTable("Test3")

      tables <- controller.retrieveTables.map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(0, tables.size())
    }
  }
}
