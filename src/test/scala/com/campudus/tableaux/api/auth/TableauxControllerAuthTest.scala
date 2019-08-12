package com.campudus.tableaux.api.auth

import java.util.NoSuchElementException

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.controller.{StructureController, TableauxController}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.Pagination
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

  def createTableauxController(
      implicit roleModel: RoleModel = initRoleModel(defaultViewTableRole)): TableauxController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val structureModel = StructureModel(dbConnection)
    val tableauxModel = TableauxModel(dbConnection, structureModel)

    TableauxController(tableauxConfig, tableauxModel, roleModel)
  }

  def createStructureController(implicit roleModel: RoleModel = RoleModel(Json.emptyObj())): StructureController = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
    val model = StructureModel(dbConnection)

    StructureController(tableauxConfig, model, roleModel)
  }

  /**
    * 1. column -> text    | single language
    * 2. column -> text    | multi language
    * 3. column -> numeric | single language
    * 4. column -> numeric | multi language
    */
  protected def createTestTable() = {
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
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
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
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
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
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
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

  @Test
  def clearCell_authorizedOnSingleLanguageColumns(implicit c: TestContext): Unit =
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.clearCellValue(1, 1, 1)
        _ <- controller.clearCellValue(1, 3, 1)
      } yield ()
    }

  @Test
  def clearCell_authorizedOnMultilanguageIfAllLangtagsAreEditable(implicit c: TestContext): Unit =
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.clearCellValue(1, 2, 1)
        _ <- controller.clearCellValue(1, 4, 1)
      } yield ()
    }

  @Test
  def clearCell_not_authorizedOnMultilanguageIfNotAllLangtagsAreEditable(implicit c: TestContext): Unit =
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
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        ex1 <- controller.clearCellValue(1, 2, 1).recover({ case ex => ex })
        ex2 <- controller.clearCellValue(1, 4, 1).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex1)
        assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex2)
      }
    }

}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_row extends TableauxControllerAuthTest {

  @Test
  def retrieveRow_filterRowsForTheirColumns_ok(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-rows": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view", "viewCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        },
                                      |        "column": {
                                      |          "kind": "text"
                                      |        },
                                      |        "langtag": "de-DE"
                                      |      }
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val structureController = createStructureController(roleModel)
      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        columns <- structureController.retrieveColumns(1).map(_.columns)
        values <- controller.retrieveRow(1, 1).map(_.values)
      } yield {
        assertEquals(2, columns.length)
        assertEquals("we have 4 columns at all, only retrieve columns with kind 'text'", 2, values.length)
      }
    }

  @Test
  def retrieveMultipleRows_filterRowsForTheirColumns_ok(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-rows": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view", "viewCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        },
                                      |        "column": {
                                      |          "kind": "text"
                                      |        },
                                      |        "langtag": "de-DE"
                                      |      }
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val structureController = createStructureController(roleModel)
      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        columns <- structureController.retrieveColumns(1).map(_.columns)
        rows <- controller.retrieveRows(1, Pagination(None, None)).map(_.rows)
      } yield {
        assertEquals(2, columns.length)
        assertEquals(2, rows.length)
        assertEquals(2, rows.head.values.length)
        assertEquals(2, rows(1).values.length)
      }
    }

  @Test
  def createRow_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val controller = createTableauxController()

      for {
        _ <- createTestTable()
        ex <- controller.createRow(1, None).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(CreateRow, ScopeTable), ex)
      }
    }

  @Test
  def createRow_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "create-rows": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["createRow"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
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

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.createRow(1, None)
      } yield ()
    }

  @Test
  def createRowWithValuesNoColumnsAreViewable_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "create-rows": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["createRow"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
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

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        ex <- controller.createRow(1, Some(Seq(Seq((1, "test"))))).recover({ case ex => ex })
      } yield {
        assertEquals(new NoSuchElementException().getClass, ex.getClass)
      }
    }

  @Test
  def createRowWithValuesColumnOneAndTwo_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "create-rows-with-values": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["createRow"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        }
                                      |      }
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view", "editCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "column": {
                                      |          "id": "1|2"
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

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.createRow(1,
                                  Some(
                                    Seq(
                                      Seq(
                                        (1, "test"),
                                        (2, Json.obj("de" -> "test_de"))
                                      ))
                                  ))
      } yield ()
    }

  @Test
  def createRowWithValuesColumnThreeAndFour_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "create-rows-with-values": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["createRow"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        }
                                      |      }
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view", "editCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "column": {
                                      |          "id": "3|4"
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

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.createRow(1,
                                  Some(
                                    Seq(
                                      Seq(
                                        (3, 13),
                                        (4, Json.obj("de" -> 37))
                                      )
                                    )))
      } yield ()
    }

  @Test
  def createRowWithValuesColumnThreeAndFour_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "create-rows-with-values": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["createRow"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
                                      |        }
                                      |      }
                                      |    },
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view", "editCellValue"],
                                      |      "scope": "column",
                                      |      "condition": {
                                      |        "column": {
                                      |          "id": "3|4"
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

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        ex1 <- controller
          .createRow(1,
                     Some(
                       Seq(
                         Seq(
                           (1, "write is permitted for column 1")
                         )
                       )))
          .recover({ case ex => ex })
        ex2 <- controller
          .createRow(1,
                     Some(
                       Seq(
                         Seq(
                           (2, Json.obj("de" -> "write is permitted for column 2"))
                         )
                       )))
          .recover({ case ex => ex })
      } yield {
        assertEquals("column 1 and 2 are not changeable", new NoSuchElementException().getClass, ex1.getClass)
        assertEquals("column 1 and 2 are not changeable", new NoSuchElementException().getClass, ex2.getClass)
      }
    }

  @Test
  def deleteRow_notAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      val controller = createTableauxController()

      for {
        _ <- createTestTable()
        ex1 <- controller.deleteRow(1, 1).recover({ case ex => ex })
        ex2 <- controller.deleteRow(1, 2).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(DeleteRow, ScopeTable), ex1)
        assertEquals(UnauthorizedException(DeleteRow, ScopeTable), ex2)
      }
    }

  @Test
  def deleteRow_authorized_ok(implicit c: TestContext): Unit =
    okTest {
      val roleModel = initRoleModel("""
                                      |{
                                      |  "create-rows": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["deleteRow"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "name": ".*"
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

      val controller = createTableauxController(roleModel)

      for {
        _ <- createTestTable()
        _ <- controller.deleteRow(1, 1)
        _ <- controller.deleteRow(1, 2)
      } yield ()
    }

}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_history extends TableauxControllerAuthTest {

  @Test
  def retrieveCellHistory_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      _ <- controller.retrieveCellHistory(1, 1, 1, None, None)
    } yield ()
  }

  @Test
  def retrieveCellHistory_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val controller = createTableauxController()

    for {
      _ <- createTestTable()
      ex <- controller.retrieveCellHistory(1, 1, 1, None, None).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex)
    }
  }

  @Test
  def retrieveRowHistory_allColumnsViewable_returnAllSevenHistoryEntries(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      historyRows <- controller.retrieveRowHistory(1, 1, None, None).map(_.getJson.getJsonArray("rows"))
    } yield {
      assertEquals(7, historyRows.size())
    }
  }

  @Test
  def retrieveRowHistory_onlyTextColumnsViewable_returnFourHistoryEntries(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "viewCellValue"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "column": {
                                    |          "kind": "text"
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

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      historyRows <- controller.retrieveRowHistory(1, 1, None, None).map(_.getJson.getJsonArray("rows"))
    } yield {
      assertEquals(4, historyRows.size())
      assertJSONEquals(Json.fromObjectString("""{"revision": 1, "event": "row_created"}"""),
                       historyRows.getJsonObject(0))
    }
  }

  @Test
  def retrieveRowHistory_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""{}""")
    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      ex <- controller.retrieveRowHistory(1, 1, None, None).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeTable), ex)
    }
  }

  @Test
  def retrieveTableHistory_onlyTextColumnsViewable_returnEightHistoryEntries(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "viewCellValue"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "column": {
                                    |          "kind": "text"
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

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      histories <- controller.retrieveTableHistory(1, None, None).map(_.getJson.getJsonArray("rows"))
    } yield {
      /* histories for two rows with each
         - 1 x rowCreated
         - 1 x single language changes
         - 2 x multi language changes
       */
      assertEquals(8, histories.size())
    }
  }

  @Test
  def retrieveTableHistory_noFilter_returnsAllHistoryEntriesOfTable(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      histories <- controller.retrieveTableHistory(1, None, None).map(_.getJson.getJsonArray("rows"))
    } yield {
      /* histories for two rows with each
             - 1 x rowCreated
             - 2 x single language changes
             - 4 x multi language changes
       */
      assertEquals(14, histories.size())
    }
  }

  @Test
  def retrieveTableHistory_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""{}""")
    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      ex <- controller.retrieveTableHistory(1, None, None).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeTable), ex)
    }
  }

}
