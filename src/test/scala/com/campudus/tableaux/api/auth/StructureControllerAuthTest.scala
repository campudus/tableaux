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
}
