package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.database.domain.{CreateSimpleColumn, DisplayInfos, GenericTable}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.database.{DatabaseConnection, LanguageNeutral, TextType}
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.router.auth.permission._
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

  def createDefaultTable(name: String): Future[Long] = {
    asDevUser(
      sendRequest("POST", "/tables", Json.obj("name" -> name))
    ).map(_.getLong("id"))
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

      tableId <- asDevUser(
        sendRequest("POST", "/tables", Json.obj("name" -> "TestTable"))
      ).map(_.getLong("id"))

      _ <- controller.deleteTable(tableId)
    } yield ()
  }

  @Test
  def deleteTable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createStructureController()

      for {
        tableId <- asDevUser(
          sendRequest("POST", "/tables", Json.obj("name" -> "TestTable"))
        ).map(_.getLong("id"))

        _ <- controller.deleteTable(tableId)
      } yield ()
    }

  @Test
  def createTable_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create"],
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
    } yield {
      assertEquals(1: Long, table.id)
      assertEquals("TestTable", table.name)
    }
  }

  @Test
  def createTable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createStructureController()

      for {
        _ <- controller.createTable("TestTable",
                                    hidden = false,
                                    langtags = None,
                                    displayInfos = DisplayInfos.fromJson(Json.emptyObj()),
                                    tableType = GenericTable,
                                    tableGroupId = None)
      } yield ()
    }

  @Test
  def createColumn_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "TestTable"))
        .map(_.getLong("id"))

      col = CreateSimpleColumn("TestColumn", None, TextType, LanguageNeutral, true, false, Nil)

      createdColumns <- controller.createColumns(tableId, Seq(col))

    } yield {
      assertEquals(1, createdColumns.columns.size)
      assertEquals(1: Long, createdColumns.columns.head.id)
      assertEquals("TestColumn", createdColumns.columns.head.name)
    }
  }

  @Test
  def createColumn_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createStructureController()

      for {
        tableId <- sendRequest("POST", "/tables", Json.obj("name" -> "TestTable"))
          .map(_.getLong("id"))

        col = CreateSimpleColumn("TestColumn", None, TextType, LanguageNeutral, true, false, Nil)

        _ <- controller.createColumns(tableId, Seq(col))
      } yield ()
    }

  @Test
  def createColumn_authorizedInModelTables_notAuthorizedInVariantTables(implicit c: TestContext): Unit = {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-columns-for-model-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        modelTableId <- createDefaultTable("test_model", 1)
        variantTableId <- createDefaultTable("test_variant", 2)

        col = CreateSimpleColumn("TestColumn", None, TextType, LanguageNeutral, true, false, Nil)

        createdColumns <- controller.createColumns(modelTableId, Seq(col)).map(_.columns)
        ex <- controller.createColumns(variantTableId, Seq(col)).recover({ case ex => ex })
      } yield {
        assertEquals("TestColumn", createdColumns.head.name)
        assertEquals(UnauthorizedException(Create, ScopeColumn), ex)
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerAuthTest_filterAuthorization extends StructureControllerAuthTest {

  @Test
  def retrieveSpecificTable_authorized_ok(implicit c: TestContext): Unit = okTest {
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
  def retrieveSpecificTable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()

      for {
        tableId <- createDefaultTable("Test")

        _ <- controller.retrieveTable(tableId)
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
  def retrieveTables_threeGenericAndOneSettingsTable_returnOnlyThreeGenericTables(implicit c: TestContext): Unit =
    okTest {
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
        _ <- asDevUser(
          for {
            _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test1"))
            _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test2"))
            _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test3", "type" -> "settings")) // not viewable
            _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test4"))

          } yield ()
        )

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

  @Test
  def retrieveSpecificColumn_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()

      for {
        tableId <- createDefaultTable()

        _ <- controller.retrieveColumn(tableId, 1)
      } yield ()
    }

  @Test
  def retrieveSpecificColumn_allColumnsViewable_ok(implicit c: TestContext): Unit = {

    val roleModel: RoleModel = initRoleModel("""
                                               |{
                                               |  "view-all-columns": [
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "column"
                                               |    }
                                               |  ]
                                               |}""".stripMargin)

    okTest {
      val controller = createStructureController(roleModel)

      for {
        tableId <- createDefaultTable()

        _ <- controller.retrieveColumn(tableId, 1)
      } yield ()
    }
  }

  @Test
  def retrieveSpecificColumn_allColumnsWithId1Viewable_ok(implicit c: TestContext): Unit = {

    val roleModel: RoleModel = initRoleModel("""
                                               |{
                                               |  "view-column-id1": [
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "column",
                                               |      "condition": {
                                               |        "column": {
                                               |          "id": "1"
                                               |        }
                                               |      }
                                               |    }
                                               |  ]
                                               |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        tableId1 <- createDefaultTable("Table1", 1)
        tableId2 <- createDefaultTable("Table2", 2)

        columnId1: Long <- controller.retrieveColumn(tableId1, 1).map(_.id)
        ex1 <- controller.retrieveColumn(tableId1, 2).recover({ case ex => ex })
        columnId2: Long <- controller.retrieveColumn(tableId2, 1).map(_.id)
        ex2 <- controller.retrieveColumn(tableId2, 2).recover({ case ex => ex })

      } yield {
        assertEquals(1, columnId1)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex1)
        assertEquals(1, columnId2)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex2)
      }
    }
  }

  @Test
  def retrieveSpecificColumn_allColumnsOfTable1Viewable_ok(implicit c: TestContext): Unit = {

    val roleModel: RoleModel = initRoleModel("""
                                               |{
                                               |  "view-columns-of-table-1": [
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "column",
                                               |      "condition": {
                                               |        "table": {
                                               |          "id": "1"
                                               |        }
                                               |      }
                                               |    }
                                               |  ]
                                               |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        tableId1 <- createDefaultTable("Table1", 1)
        tableId2 <- createDefaultTable("Table2", 2)

        columnId1: Long <- controller.retrieveColumn(tableId1, 1).map(_.id)
        columnId2: Long <- controller.retrieveColumn(tableId1, 2).map(_.id)
        ex1 <- controller.retrieveColumn(tableId2, 1).recover({ case ex => ex })
        ex2 <- controller.retrieveColumn(tableId2, 2).recover({ case ex => ex })

      } yield {
        assertEquals(1, columnId1)
        assertEquals(2, columnId2)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex1)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex2)
      }
    }
  }

  @Test
  def retrieveSpecificColumn_onlyColumnWithId1AndOfTable1Viewable_ok(implicit c: TestContext): Unit = {

    val roleModel: RoleModel = initRoleModel("""
                                               |{
                                               |  "view-columns-of-table-1": [
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "column",
                                               |      "condition": {
                                               |        "table": {
                                               |          "id": "1"
                                               |        },
                                               |        "column": {
                                               |          "id": "1"
                                               |        }
                                               |      }
                                               |    }
                                               |  ]
                                               |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        tableId1 <- createDefaultTable("Table1", 1)
        tableId2 <- createDefaultTable("Table2", 2)

        columnId1: Long <- controller.retrieveColumn(tableId1, 1).map(_.id)
        ex1 <- controller.retrieveColumn(tableId1, 2).recover({ case ex => ex })
        ex2 <- controller.retrieveColumn(tableId2, 1).recover({ case ex => ex })
        ex3 <- controller.retrieveColumn(tableId2, 2).recover({ case ex => ex })

      } yield {
        assertEquals(1, columnId1)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex1)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex2)
        assertEquals(UnauthorizedException(View, ScopeColumn), ex3)
      }
    }
  }

  @Test
  def retrieveColumns_noViewPermission_returnEmptyList(implicit c: TestContext): Unit = {
    okTest {
      val controller = createStructureController()
      for {
        tableId <- createDefaultTable("Table1", 1)

        columns <- controller.retrieveColumns(tableId).map(_.getJson.getJsonArray("columns", Json.emptyArr()))

      } yield {
        assertEquals(0, columns.size())
      }
    }
  }

  @Test
  def retrieveColumns_onlyColumnsWithKindNumericAreViewable_ok(implicit c: TestContext): Unit = {

    val roleModel: RoleModel = initRoleModel("""
                                               |{
                                               |  "view-numeric-columns": [
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "column",
                                               |      "condition": {
                                               |        "column": {
                                               |          "kind": "numeric"
                                               |        }
                                               |      }
                                               |    }
                                               |  ]
                                               |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        tableId <- createDefaultTable("Table1", 1)

        columns <- controller.retrieveColumns(tableId).map(_.getJson.getJsonArray("columns", Json.emptyArr()))

      } yield {
        assertEquals(1, columns.size())
        assertEquals("numeric", columns.get(0).asInstanceOf[JsonObject].getString("kind"))
      }
    }
  }

}
