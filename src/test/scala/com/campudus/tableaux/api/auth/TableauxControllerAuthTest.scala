package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.{StructureModel, TableauxModel}
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.testtools.RequestCreation.{Identifier, Multilanguage, NumericCol, TextCol}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

trait TableauxControllerAuthTest extends TableauxTestBase {

  def createTableauxController(roleModel: RoleModel = RoleModel(Json.emptyObj())): TableauxController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel, roleModel)

    TableauxController(tableauxConfig, tableauxModel, roleModel)
  }
}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_cell extends TableauxControllerAuthTest {

  @Test
  def retrieveCell_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "cell"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      tableId <- createDefaultTable()
      _ <- controller.retrieveCell(tableId, 1, 1)
    } yield ()
  }

  @Test
  def retrieveCell_authorizedInModelTables_notAuthorizedInVariantTables(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "cell",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      modelTableId <- createDefaultTable("test_model", 1)
      variantTableId <- createDefaultTable("test_variant", 2)
      _ <- controller.retrieveCell(modelTableId, 1, 1)
      ex <- controller.retrieveCell(variantTableId, 1, 1).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeCell), ex)
    }
  }

  @Test
  def retrieveCell_authorizedInModelTableAndTextColumns(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "cell",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*_model"
                                    |        },
                                    |        "column": {
                                    |          "kind": "text"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      modelTableId <- createDefaultTable("test_model", 1)
      variantTableId <- createDefaultTable("test_variant", 2)

      _ <- controller.retrieveCell(modelTableId, 1, 1)
      ex1 <- controller.retrieveCell(modelTableId, 2, 1).recover({ case ex => ex })
      ex2 <- controller.retrieveCell(variantTableId, 1, 1).recover({ case ex => ex })
      ex3 <- controller.retrieveCell(variantTableId, 2, 1).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeCell), ex1)
      assertEquals(UnauthorizedException(View, ScopeCell), ex2)
      assertEquals(UnauthorizedException(View, ScopeCell), ex3)
    }
  }

  @Test
  def retrieveCell_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val controller = createTableauxController()

      for {
        tableId <- createDefaultTable()
        ex <- controller.retrieveCell(tableId, 1, 1).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(View, ScopeCell), ex)
      }
    }

  @Test
  def retrieveCell_langtagConditionShouldNotAffectActionView(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "cell",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        },
                                      |        "langtag": "de"
                                      |      }
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {

        _ <- createSimpleTableWithValues(
          "table",
          List(TextCol("text"), Multilanguage(TextCol("multilanguage_text")), NumericCol("numeric")),
          List(
            List("test1", Json.obj("de" -> "test1-de", "en" -> "test1-en"), 1),
            List("test2", Json.obj("de" -> "test2-de", "en" -> "test2-en"), 2)
          )
        )

        _ <- controller.retrieveCell(1, 1, 1)
        _ <- controller.retrieveCell(1, 2, 1)
        _ <- controller.retrieveCell(1, 3, 1)
      } yield ()
    }

}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_row extends TableauxControllerAuthTest {}
