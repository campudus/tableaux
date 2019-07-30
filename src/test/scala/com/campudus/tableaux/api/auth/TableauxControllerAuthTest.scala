package com.campudus.tableaux.api.auth

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.TableauxController
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.model.{StructureModel, TableauxModel}
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.testtools.RequestCreation.{Multilanguage, NumericCol, TextCol}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

trait TableauxControllerAuthTest extends TableauxTestBase {

  def createTableauxController(implicit roleModel: RoleModel = RoleModel(Json.emptyObj())): TableauxController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)

    TableauxController(tableauxConfig, tableauxModel, roleModel)
  }
}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_cell extends TableauxControllerAuthTest {

  /**
    * 1. column -> text    | single language
    * 2. column -> text    | multi language
    * 3. column -> numeric | single language
    * 4. column -> numeric | multi language
    */
  private def createTestTable() = {
    createSimpleTableWithValues(
      "table",
      List(TextCol("text"),
           Multilanguage(TextCol("multilanguage_text")),
           NumericCol("numeric"),
           Multilanguage(NumericCol("multilanguage_numeric"))),
      List(
        List("test1", Json.obj("de" -> "test1-de", "en" -> "test1-en"), 1, Json.obj("de" -> 2, "en" -> 3)),
        List("test2", Json.obj("de" -> "test2-de", "en" -> "test2-en"), 2, Json.obj("de" -> 2, "en" -> 3))
      )
    )
  }

  @Test
  def retrieveCell_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
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
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column",
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
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex)
    }
  }

  @Test
  def retrieveCell_authorizedInModelTableAndTextColumns(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column",
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
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex1)
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex2)
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex3)
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
        assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex)
      }
    }

  @Test
  def retrieveCell_langtagConditionMustNotBeConsideredOnActionView(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["viewCellValue"],
                                      |      "scope": "column",
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

        _ <- createTestTable()

        _ <- controller.retrieveCell(1, 1, 1)
        _ <- controller.retrieveCell(1, 2, 1)
        _ <- controller.retrieveCell(1, 3, 1)
      } yield ()
    }

  @Test
  def updateCell_langtagConditionMustNotBeConsideredOnActionView(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editCellValue", "viewCellValue"],
                                      |      "scope": "column",
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
        _ <- createTestTable()

        _ <- controller.updateCellValue(1, 1, 1, "new text")

        _ <- controller.updateCellValue(1, 2, 1, Json.obj("de" -> "value-de"))
        ex1 <- controller.updateCellValue(1, 2, 1, Json.obj("en" -> "value-en")).recover({ case ex => ex })
        ex2 <- controller
          .updateCellValue(1, 2, 1, Json.obj("de" -> "value-de", "en" -> "value-en"))
          .recover({ case ex => ex })

        _ <- controller.updateCellValue(1, 3, 1, 42)
      } yield {
        assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex1)
        assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex2)
      }
    }

  @Test
  def replaceCell_authorizedOnSingleLanguageColumns(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editCellValue", "viewCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        },
                                      |        "langtag": "de-DE"
                                      |      }
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.replaceCellValue(1, 1, 1, "new text")
        _ <- controller.replaceCellValue(1, 3, 1, 42)
      } yield ()
    }

  @Test
  def replaceCell_authorizedOnMultilanguageIfAllLangtagsAreEditable(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editCellValue", "viewCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        },
                                      |        "langtag": "de-DE|en-GB"
                                      |      }
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.replaceCellValue(1, 2, 1, Json.obj("de-DE" -> "value-de"))
        _ <- controller.replaceCellValue(1, 4, 1, Json.obj("en-GB" -> 1))
      } yield ()
    }

  @Test
  def replaceCell_not_authorizedOnMultilanguageIfNotAllLangtagsAreEditable(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "edit-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["editCellValue", "viewCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        },
                                      |        "langtag": "de-DE"
                                      |      }
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        ex1 <- controller.replaceCellValue(1, 2, 1, Json.obj("en-GB" -> "value-en")).recover({ case ex => ex })
        ex2 <- controller.replaceCellValue(1, 4, 1, Json.obj("de-DE" -> 1)).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex1)
        assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex2)
      }
    }

}

//@RunWith(classOf[VertxUnitRunner])
//class TableauxControllerAuthTest_row extends TableauxControllerAuthTest {}
