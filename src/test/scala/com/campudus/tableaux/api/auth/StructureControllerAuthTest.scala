package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.StructureController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{CreateSimpleColumn, DisplayInfos, DomainObject, GenericTable}
import com.campudus.tableaux.database.model.StructureModel
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.helper.JsonUtils._
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.skyscreamer.jsonassert.JSONCompareMode
import org.vertx.scala.core.json.{Json, JsonObject}

trait StructureControllerAuthTest extends TableauxTestBase {

  val defaultViewTableRole = """
                               |{
                               |  "view-all-tables": [
                               |    {
                               |      "type": "grant",
                               |      "action": ["view"],
                               |      "scope": "table"
                               |    }
                               |  ]
                               |}""".stripMargin

  def createStructureController(
      implicit roleModel: RoleModel = initRoleModel(defaultViewTableRole)): StructureController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model, roleModel)
  }

  val displayInfoJson =
    Json.obj(
      "displayName" -> Json.obj("de" -> "Name"),
      "description" -> Json.obj("de" -> "Beschreibung")
    )

  def getPermission(domainObject: DomainObject): JsonObject = {
    domainObject.getJson.getJsonObject("permission")
  }
}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerTableAuthTest_checkAuthorization extends StructureControllerAuthTest {

  @Test
  def deleteTable_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "delete-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "delete"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {

      tableId <- createDefaultTable("Test")

      _ <- controller.deleteTable(tableId)
    } yield ()
  }

  @Test
  def deleteTable_modelAllowedOthersNot(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "delete-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "delete"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        }
                                    |      }
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {

      modelTableId <- createDefaultTable("test_model", 1)
      variantTableId <- createDefaultTable("test_variant", 1)

      _ <- controller.deleteTable(modelTableId)
      ex <- controller.deleteTable(variantTableId).recover({ case ex => ex })
    } yield {
      // before deletion, retrieve table is called
      // so UnauthorizedException for Deletion is only thrown if we can view this table
      // otherwise we even get an UnauthorizedException for action View
      assertEquals(UnauthorizedException(Delete, ScopeTable), ex)
    }
  }

  @Test
  def deleteTable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createStructureController()

      for {
        tableId <- createDefaultTable("Test")

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
                                    |      "action": ["view", "create"],
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
                                      tableGroupId = None,
                                      attributes = None)
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
                                    tableGroupId = None,
                                    attributes = None)
      } yield ()
    }

  @Test
  def changeTableDisplayProperties_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "change-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)
    val displayInfos = DisplayInfos.fromJson(displayInfoJson)

    for {
      tableId <- createDefaultTable("Test")

      _ <- controller.changeTable(tableId, None, None, None, Some(displayInfos), None, None)
    } yield ()
  }

  @Test
  def changeTableDisplayProperties_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()
      val displayInfos = DisplayInfos.fromJson(displayInfoJson)

      for {
        tableId <- createDefaultTable("Test")
        _ <- controller.changeTable(tableId, None, None, None, Some(displayInfos), None, None)
      } yield ()
    }

  @Test
  def changeTableStructureProperties_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "change-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editStructureProperty"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable("Test")
      _ <- controller.changeTable(tableId, Some("changeTableName"), None, None, None, None, None)
    } yield ()
  }

  @Test
  def changeTableStructureProperties_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "change-tables": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editDisplayProperty"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createStructureController(roleModel)

      for {
        tableId <- createDefaultTable("Test")

        _ <- controller.changeTable(tableId, Some("changeTableName"), None, None, None, None, None)
      } yield ()
    }

  @Test
  def changeTableOrder_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "change-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable("Test")
      _ <- controller.changeTableOrder(tableId, LocationType("start", None))
    } yield ()
  }

  @Test
  def changeTableOrder_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()

      for {
        tableId <- createDefaultTable("Test")

        _ <- controller.changeTableOrder(tableId, LocationType("start", None))
      } yield ()
    }

}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerTableGroupAuthTest_checkAuthorization extends StructureControllerAuthTest {

  @Test
  def createTableGroup_authorized_ok(implicit c: TestContext): Unit = {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-table-group": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create"],
                                    |      "scope": "tableGroup"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)
    val displayInfos = DisplayInfos.fromJson(displayInfoJson)

    okTest {
      for {
        _ <- controller.createTableGroup(displayInfos)
      } yield ()
    }
  }

  @Test
  def createTableGroup_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()
      val displayInfos = DisplayInfos.fromJson(displayInfoJson)

      for {
        _ <- controller.createTableGroup(displayInfos)
      } yield ()
    }

  @Test
  def updateTableGroup_authorized_ok(implicit c: TestContext): Unit = {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-table-group": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["edit"],
                                    |      "scope": "tableGroup"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)
    val newInfos = DisplayInfos.fromJson(Json.obj("displayName" -> Json.obj("de" -> "new Name")))

    okTest {
      for {
        groupId <- sendRequest("POST", "/groups", displayInfoJson).map(_.getLong("id"))
        _ <- controller.changeTableGroup(groupId, Some(newInfos))
      } yield ()
    }
  }

  @Test
  def updateTableGroup_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()
      val newInfos = DisplayInfos.fromJson(Json.obj("displayName" -> Json.obj("de" -> "new Name")))

      for {
        groupId <- sendRequest("POST", "/groups", displayInfoJson).map(_.getLong("id"))
        _ <- controller.changeTableGroup(groupId, Some(newInfos))
      } yield ()
    }

  @Test
  def deleteTableGroup_authorized_ok(implicit c: TestContext): Unit = {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "delete-table-group": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["delete"],
                                    |      "scope": "tableGroup"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        groupId <- sendRequest("POST", "/groups", displayInfoJson).map(_.getLong("id"))
        _ <- controller.deleteTableGroup(groupId)
      } yield ()
    }
  }

  @Test
  def deleteTableGroup_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()

      for {
        groupId <- sendRequest("POST", "/groups", displayInfoJson).map(_.getLong("id"))
        _ <- controller.deleteTableGroup(groupId)
      } yield ()
    }

}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerColumnAuthTest_checkAuthorization extends StructureControllerAuthTest {

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
      tableId <- createDefaultTable("Test")

      col = CreateSimpleColumn("TestColumn", None, TextType, LanguageNeutral, true, Nil, false, None)

      createdColumns <- controller.createColumns(tableId, Seq(col))

    } yield {
      assertEquals(1, createdColumns.columns.size)
      assertEquals(3: Long, createdColumns.columns.head.id)
      assertEquals("TestColumn", createdColumns.columns.head.name)
    }
  }

  @Test
  def createColumn_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {

      val controller = createStructureController()

      for {
        tableId <- createDefaultTable("Test")

        col = CreateSimpleColumn("TestColumn", None, TextType, LanguageNeutral, true, Nil, false, None)

        _ <- controller.createColumns(tableId, Seq(col))
      } yield ()
    }

  @Test
  def createColumn_authorizedInModelTables_notAuthorizedInVariantTables(implicit c: TestContext): Unit = {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-columns-in-model-tables": [
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

        col = CreateSimpleColumn("TestColumn", None, TextType, LanguageNeutral, true, Nil, false, None)

        createdColumns <- controller.createColumns(modelTableId, Seq(col)).map(_.columns)
        ex <- controller.createColumns(variantTableId, Seq(col)).recover({ case ex => ex })
      } yield {
        assertEquals("TestColumn", createdColumns.head.name)
        assertEquals(UnauthorizedException(Create, ScopeColumn), ex)
      }
    }
  }

  @Test
  def deleteColumn_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "delete-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["delete"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable("Test")
      _ <- controller.deleteColumn(tableId, 1)
    } yield ()
  }

  @Test
  def deleteColumn_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()

      for {
        tableId <- createDefaultTable("Test")
        _ <- controller.deleteColumn(tableId, 1)
      } yield ()
    }

  @Test
  def changeColumnEditDisplayProperty_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editDisplayProperty"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)
    val displayInfos = DisplayInfos.fromJson(displayInfoJson)

    for {
      tableId <- createDefaultTable("Test")
      _ <- controller.changeColumn(tableId, 1, None, None, None, None, Some(displayInfos), None, None, None)
    } yield ()
  }

  @Test
  def changeColumnEditDisplayProperty_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()
      val displayInfos = DisplayInfos.fromJson(displayInfoJson)

      for {
        tableId <- createDefaultTable("Test")
        _ <- controller.changeColumn(tableId, 1, None, None, None, None, Some(displayInfos), None, None, None)
      } yield ()
    }

  @Test
  def changeColumnEditStructureProperty_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editStructureProperty"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)
    val displayInfos = DisplayInfos.fromJson(displayInfoJson)

    for {
      tableId <- createDefaultTable("Test")
      _ <- controller.changeColumn(tableId, 1, Some("newName"), None, None, None, Some(displayInfos), None, None, None)
    } yield ()
  }

  @Test
  def changeColumnEditStructureProperty_notAuthorized_throwsException(implicit c: TestContext): Unit =
    exceptionTest("error.request.unauthorized") {
      val controller = createStructureController()
      val displayInfos = DisplayInfos.fromJson(displayInfoJson)

      for {
        tableId <- createDefaultTable("Test")
        _ <- controller
          .changeColumn(tableId, 1, Some("newName"), None, None, None, Some(displayInfos), None, None, None)
      } yield ()
    }

  @Test
  def changeColumnEditStructureProperty_authorizedInModelTables_notAuthorizedInVariantTables(
      implicit c: TestContext): Unit = {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns-in-model-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editStructureProperty"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        }
                                    |      }
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        modelTableId <- createDefaultTable("test_model", 1)
        variantTableId <- createDefaultTable("test_variant", 2)

        _ <- controller.changeColumn(modelTableId, 1, Some("newName"), None, None, None, None, None, None, None)
        ex <- controller
          .changeColumn(variantTableId, 1, Some("newName"), None, None, None, None, None, None, None)
          .recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(EditStructureProperty, ScopeColumn), ex)
      }
    }
  }

  @Test
  def deleteColumn_authorizedInModelTables_notAuthorizedInVariantTables(implicit c: TestContext): Unit = {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "delete-columns-in-model-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["delete"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        }
                                    |      }
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        modelTableId <- createDefaultTable("test_model", 1)
        variantTableId <- createDefaultTable("test_variant", 2)

        _ <- controller.deleteColumn(modelTableId, 1)
        ex <- controller.deleteColumn(variantTableId, 1).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(Delete, ScopeColumn), ex)
      }
    }
  }

  @Test
  def deleteColumn_authorizedForIdentifier_notAuthorizedForNonIdentifier(implicit c: TestContext): Unit = {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "delete-columns-in-model-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["delete"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "column": {
                                    |          "identifier": "true"
                                    |        }
                                    |      }
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        tableId <- createDefaultTable("test_model", 1)

        _ <- controller.deleteColumn(tableId, 1) // "identifier" == true
        ex <- controller.deleteColumn(tableId, 2).recover({ case ex => ex }) // "identifier" != true
      } yield {
        assertEquals(UnauthorizedException(Delete, ScopeColumn), ex)
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
      val roleModel: RoleModel = initRoleModel("""{}""")
      val controller = createStructureController(roleModel)

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
        _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test1"))
        _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test2"))
        _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test3", "type" -> "settings")) // not viewable
        _ <- sendRequest("POST", "/tables", Json.obj("name" -> "Test4"))

        tables <- controller.retrieveTables().map(_.getJson.getJsonArray("tables", Json.emptyArr()))
      } yield {
        assertEquals(3, tables.size())

        val tableIds = asSeqOf[JsonObject](tables).map(_.getInteger("id"))
        assertEquals(Seq(1, 2, 4), tableIds)
      }
    }

  @Test
  def retrieveTables_noViewPermission_returnEmptyList(implicit c: TestContext): Unit = okTest {

    val roleModel: RoleModel = initRoleModel("""{}""")
    val controller = createStructureController(roleModel)

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
                                               |    },
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "table"
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
                                               |    },
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "table"
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
                                               |    },
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "table"
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
                                               |    },
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "table"
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

        columns <- controller.retrieveColumns(tableId).map(_.columns)

      } yield {
        assertEquals(0, columns.length)
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
                                               |    },
                                               |    {
                                               |      "type": "grant",
                                               |      "action": ["view"],
                                               |      "scope": "table"
                                               |    }
                                               |  ]
                                               |}""".stripMargin)

    val controller = createStructureController(roleModel)

    okTest {
      for {
        tableId <- createDefaultTable("Table1", 1)

        columns <- controller.retrieveColumns(tableId).map(_.columns)

      } yield {
        assertEquals(1, columns.length)
        assertEquals(NumericType, columns.head.kind)
      }
    }
  }

  @Test
  def retrieveColumns_onlyColumnsFromModelTableAreViewable_ok(implicit c: TestContext): Unit = {
    okTest {

      val roleModel: RoleModel = initRoleModel("""
                                                 |{
                                                 |  "view-columns-from-model-tables": [
                                                 |    {
                                                 |      "type": "grant",
                                                 |      "action": ["view"],
                                                 |      "scope": "column",
                                                 |      "condition": {
                                                 |        "table": {
                                                 |          "name": ".*_model"
                                                 |        },
                                                 |        "column": {
                                                 |          "id": ".*"
                                                 |        }
                                                 |      }
                                                 |    },
                                                 |    {
                                                 |      "type": "grant",
                                                 |      "action": ["view"],
                                                 |      "scope": "table"
                                                 |    }
                                                 |  ]
                                                 |}""".stripMargin)

      val controller = createStructureController(roleModel)

      for {
        modelTableId <- createDefaultTable("test_model", 1)
        variantTableId <- createDefaultTable("test_variant", 2)

        modelTableColumns <- controller.retrieveColumns(modelTableId).map(_.columns)
        variantTableColumns <- controller.retrieveColumns(variantTableId).map(_.columns)
      } yield {
        assertEquals(2, modelTableColumns.length)
        assertEquals(0, variantTableColumns.length)
      }
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class StructureControllerAuthTest_enrichDomainObjects extends StructureControllerAuthTest {

  @Test
  def enrichTableSeq_createIsAllowed(implicit c: TestContext): Unit = okTest {
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
      permission <- controller.retrieveTables().map(getPermission)
    } yield {

      val expected = Json.obj(
        "create" -> true,
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTableSeq_createIsNotAllowed(implicit c: TestContext): Unit = okTest {
    val controller = createStructureController()

    for {
      permission <- controller.retrieveTables().map(getPermission)
    } yield {

      val expected = Json.obj(
        "create" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_editPropertiesAreAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty", "editStructureProperty"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveTable(tableId).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_editStructureProperty_onlyForModelTablesAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editStructureProperty"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      modelTableId <- createDefaultTable("test_model", 1)
      variantTableId <- createDefaultTable("test_variant", 2)
      modelTablePermissions <- controller.retrieveTable(modelTableId).map(getPermission)
      variantTablePermissions <- controller.retrieveTable(variantTableId).map(getPermission)
    } yield {

      val modelTableExpected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true
      )

      val variantTableExpected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> false
      )

      assertJSONEquals(modelTableExpected, modelTablePermissions, JSONCompareMode.LENIENT)
      assertJSONEquals(variantTableExpected, variantTablePermissions, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_noActionIsAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveTable(tableId).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> false,
        "editStructureProperty" -> false,
        "delete" -> false,
        "createRow" -> false,
        "deleteRow" -> false,
        "editCellAnnotation" -> false,
        "editRowAnnotation" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichTable_allActionsAreAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-tables": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty", "editStructureProperty", "delete",
                                    |        "createRow", "deleteRow", "editCellAnnotation", "editRowAnnotation"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "create"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveTable(tableId).map(getPermission)
    } yield {

      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true,
        "createRow" -> true,
        "deleteRow" -> true,
        "editCellAnnotation" -> true,
        "editRowAnnotation" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumnSeq_createIsAllowed(implicit c: TestContext): Unit = okTest {
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
                                    |      "action": ["view", "create"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveColumns(tableId).map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> true,
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumnSeq_createIsNotAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "create-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveColumns(tableId).map(getPermission)
    } yield {
      val expected = Json.obj(
        "create" -> false,
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_noActionIsAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveColumn(tableId, 1).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> false,
        "editStructureProperty" -> false,
        "delete" -> false,
        "editCellValue" -> false
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_allActionsAreAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editDisplayProperty", "editStructureProperty", "delete",
                                    |        "editCellValue"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      tableId <- createDefaultTable()
      permission <- controller.retrieveColumn(tableId, 1).map(getPermission)
    } yield {
      val expected = Json.obj(
        "editDisplayProperty" -> true,
        "editStructureProperty" -> true,
        "delete" -> true,
        "editCellValue" -> true
      )

      assertJSONEquals(expected, permission, JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_editCellValueIsAllowedForLangtagsDeAndEs(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editCellValue"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "langtag": "en|es"
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      _ <- sendRequest("POST", "/system/settings/langtags", Json.obj("value" -> Json.arr("de", "en", "fr", "es")))

      _ <- createFullTableWithMultilanguageColumns("Test Table")
      permission1 <- controller.retrieveColumn(1, 1).map(getPermission)
      permission2 <- controller.retrieveColumn(1, 2).map(getPermission)
    } yield {
      val expected = Json.obj("de" -> false, "en" -> true, "fr" -> false, "es" -> true)

      assertJSONEquals(expected, permission1.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
      assertJSONEquals(expected, permission2.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_editCellValueWithoutLangtagConditionIsAllowedForAllLangtags(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editCellValue"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    for {
      _ <- sendRequest("POST", "/system/settings/langtags", Json.obj("value" -> Json.arr("de", "en", "fr", "es")))

      _ <- createFullTableWithMultilanguageColumns("Test Table")
      permission <- controller.retrieveColumn(1, 1).map(getPermission)
    } yield {
      val expected = Json.obj("de" -> true, "en" -> true, "fr" -> true, "es" -> true)

      assertJSONEquals(expected, permission.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
    }
  }

  @Test
  def enrichColumn_editCellValueWithoutLangtagConditionIsAllowedForAllCountryTags(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-columns": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view", "editCellValue"],
                                      |      "scope": "column"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createStructureController(roleModel)

      val country_column = Json.obj(
        "name" -> "country_column",
        "kind" -> "currency",
        "languageType" -> "country",
        "countryCodes" -> Json.arr("DE", "AT", "GB")
      )

      val columns = Json.obj("columns" -> Json.arr(country_column))

      for {
        _ <- createEmptyDefaultTable()
        _ <- sendRequest("POST", s"/tables/1/columns", columns)

        permission <- controller.retrieveColumn(1, 3).map(getPermission)
      } yield {
        val expected = Json.obj("DE" -> true, "AT" -> true, "GB" -> true)
        assertJSONEquals(expected, permission.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
      }
    }

  @Test
  def enrichColumn_editCellValueIsAllowedForCountryTagsATAndGB(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "edit-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editCellValue"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "langtag": "AT|GB"
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createStructureController(roleModel)

    val country_column = Json.obj(
      "name" -> "country_column",
      "kind" -> "currency",
      "languageType" -> "country",
      "countryCodes" -> Json.arr("DE", "AT", "GB")
    )

    val columns = Json.obj("columns" -> Json.arr(country_column))

    for {
      _ <- createEmptyDefaultTable()
      _ <- sendRequest("POST", s"/tables/1/columns", columns)

      permission <- controller.retrieveColumn(1, 3).map(getPermission)
    } yield {
      val expected = Json.obj("DE" -> false, "AT" -> true, "GB" -> true)
      assertJSONEquals(expected, permission.getJsonObject("editCellValue"), JSONCompareMode.LENIENT)
    }
  }
}
