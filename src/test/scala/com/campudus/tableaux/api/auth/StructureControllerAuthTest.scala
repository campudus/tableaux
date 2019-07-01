package com.campudus.tableaux.api.auth

import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.database.DatabaseConnection
import com.campudus.tableaux.database.domain.{DisplayInfos, GenericTable, Table}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.router.auth.permission.RoleModel
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future

trait StructureControllerAuthTest extends TableauxTestBase {

  def createStructureController(roleModel: RoleModel = RoleModel(Json.emptyObj())): StructureController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model, roleModel)
  }

  def createDefaultTable(name: String): Future[Table] = {
    val controller = createStructureController()

    controller.createGenericTable(name,
                                  hidden = false,
                                  langtags = None,
                                  displayInfos = DisplayInfos.fromJson(Json.emptyObj()),
                                  tableGroupId = None)
  }

}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerAuthTest_checkAuthorization extends StructureControllerAuthTest {

  @Test
  def deleteTable_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
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

      val controller = createStructureController()

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
}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerAuthTest_filterAuthorization extends StructureControllerAuthTest {

  @Test
  def retrieveTable_authorized_ok(implicit c: TestContext): Unit = okTest {
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
      _ <- createDefaultTable("Test")

      tableId <- controller.retrieveTable(1).map(_.id)
    } yield {
      assertEquals(1: TableId, tableId)
    }
  }

  @Test
  def retrieveTable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()

      for {
        _ <- createDefaultTable("Test")

        tableId <- controller.retrieveTable(1).map(_.id)
      } yield ()
    }

  @Test
  def retrieveTables_threeTablesAllViewable_returnAll(implicit c: TestContext): Unit = okTest {
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

      tables <- controller.retrieveTables().map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(3, tables.size())
    }
  }

  @Test
  def retrieveTables_threeTablesTwoViewable_returnTwo(implicit c: TestContext): Unit = okTest {
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

      tables <- controller.retrieveTables().map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(2, tables.size())

      val tableIds = asSeqOf[JsonObject](tables).map(_.getInteger("id"))
      assertEquals(Seq(1, 3), tableIds)
    }
  }

  @Test
  def retrieveTables__returnTwo(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-generic-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "deny",
                                    |      "action": ["view"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "tableType": "settings"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      _ <- createDefaultTable("Test1")
      _ <- createDefaultTable("Test2")
      _ <- controller.createSettingsTable("Test3", // not viewable
                                          hidden = false,
                                          langtags = None,
                                          displayInfos = DisplayInfos.fromJson(Json.emptyObj()),
                                          tableGroupId = None)
      _ <- createDefaultTable("Test4")

      tables <- controller.retrieveTables().map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(3, tables.size())

      val tableIds = asSeqOf[JsonObject](tables).map(_.getInteger("id"))
      assertEquals(Seq(1, 2, 4), tableIds)
    }
  }

  @Test
  def retrieveTables_noViewPermission_returnEmptyList(implicit c: TestContext): Unit = okTest {

    val controller = createStructureController()

    for {
      _ <- createDefaultTable("Test1")
      _ <- createDefaultTable("Test2")
      _ <- createDefaultTable("Test3")

      tables <- controller.retrieveTables().map(_.getJson.getJsonArray("tables", Json.emptyArr()))
    } yield {
      assertEquals(0, tables.size())
    }
  }
}
