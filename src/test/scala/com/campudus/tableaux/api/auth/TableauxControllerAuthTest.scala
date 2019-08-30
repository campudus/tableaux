package com.campudus.tableaux.api.auth

import java.util.{NoSuchElementException, UUID}

import com.campudus.tableaux.UnauthorizedException
import com.campudus.tableaux.api.content.LinkTestBase
import com.campudus.tableaux.api.media.MediaTestBase
import com.campudus.tableaux.controller.{StructureController, TableauxController}
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{InfoAnnotationType, Pagination}
import com.campudus.tableaux.database.model.TableauxModel.TableId
import com.campudus.tableaux.database.model.{StructureModel, TableauxModel}
import com.campudus.tableaux.helper.JsonUtils.{toCreateColumnSeq, toRowValueSeq}
import com.campudus.tableaux.router.auth.permission._
import com.campudus.tableaux.testtools.RequestCreation._
import com.campudus.tableaux.testtools.{RequestCreation, TableauxTestBase}
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}

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
  protected def createTestTable(tableName: String = "table") = {
    createSimpleTableWithValues(
      tableName,
      List(Identifier(TextCol("text")),
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
  def deleteRow_authorized_ok(implicit c: TestContext): Unit = okTest {
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

  @Test
  def retrieveRowsOfColumn_authorized_ok(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-rows": [
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
      completeRows <- controller.retrieveRows(1, Pagination(None, None)).map(_.rows)

      rows1 <- controller.retrieveRowsOfColumn(1, 1, Pagination(None, None)).map(_.rows)
      rows2 <- controller.retrieveRowsOfColumn(1, 2, Pagination(None, None)).map(_.rows)
      rows3 <- controller.retrieveRowsOfColumn(1, 3, Pagination(None, None)).map(_.rows)
      rows4 <- controller.retrieveRowsOfColumn(1, 4, Pagination(None, None)).map(_.rows)
    } yield {
      assertEquals(4, completeRows.head.values.length)
      assertEquals(1, rows1.head.values.length)
      assertEquals(1, rows2.head.values.length)
      assertEquals(1, rows3.head.values.length)
      assertEquals(1, rows4.head.values.length)
    }
  }

  @Test
  def retrieveRowsOfColumn_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-rows": [
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

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      ex1 <- controller.retrieveRowsOfColumn(1, 1, Pagination(None, None)).recover({ case ex => ex })
      ex2 <- controller.retrieveRowsOfColumn(1, 2, Pagination(None, None)).recover({ case ex => ex })
      ex3 <- controller.retrieveRowsOfColumn(1, 3, Pagination(None, None)).recover({ case ex => ex })
      ex4 <- controller.retrieveRowsOfColumn(1, 4, Pagination(None, None)).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex1)
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex2)
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex3)
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex4)
    }
  }

  @Test
  def retrieveRowsOfFirstColumn_authorized_ok(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-rows": [
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
      completeRows <- controller.retrieveRows(1, Pagination(None, None)).map(_.rows)

      rowsOfFirstColumn <- controller.retrieveRowsOfFirstColumn(1, Pagination(None, None)).map(_.rows)
    } yield {
      assertEquals(4, completeRows.head.values.length)
      assertEquals(1, rowsOfFirstColumn.head.values.length)
    }
  }

  @Test
  def retrieveRowsOfFirstColumn_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-rows": [
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

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      ex <- controller.retrieveRowsOfFirstColumn(1, Pagination(None, None)).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex)
    }
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

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_annotation extends TableauxControllerAuthTest {

  def toUuid(jsonObject: JsonObject): String = jsonObject.getString("uuid")

  @Test
  def createCellAnnotation_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editCellAnnotation"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      _ <- controller.addCellAnnotation(1, 1, 1, Seq.empty[String], InfoAnnotationType, "my info annotation")
      _ <- controller.addCellAnnotation(1, 2, 1, Seq("de"), InfoAnnotationType, "my info annotation-de")
    } yield ()
  }

  @Test
  def retrieveCellHistory_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
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
      ex1 <- controller
        .addCellAnnotation(1, 1, 1, Seq.empty[String], InfoAnnotationType, "my info annotation")
        .recover({ case ex => ex })
      ex2 <- controller
        .addCellAnnotation(1, 2, 1, Seq("de"), InfoAnnotationType, "my info annotation-de")
        .recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditCellAnnotation, ScopeTable), ex1)
      assertEquals(UnauthorizedException(EditCellAnnotation, ScopeTable), ex2)
    }
  }

  @Test
  def deleteCellAnnotation_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editCellAnnotation"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    val langtagInfoAnnotation = Json.fromObjectString("""{"langtags": ["de", "en"], "type": "info"}""")
    val infoAnnotation = Json.fromObjectString("""{"type": "info", "value": "this is a comment"}""")

    for {
      _ <- createTestTable()

      uuid1 <- sendRequest("POST", s"/tables/1/columns/2/rows/1/annotations", langtagInfoAnnotation).map(toUuid)
      uuid2 <- sendRequest("POST", s"/tables/1/columns/1/rows/1/annotations", infoAnnotation).map(toUuid)

      _ <- controller.deleteCellAnnotation(1, 1, 1, UUID.fromString(uuid1))
      _ <- controller.deleteCellAnnotation(1, 2, 1, UUID.fromString(uuid2))
    } yield ()
  }

  @Test
  def deleteCellHistory_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
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

    val langtagInfoAnnotation = Json.fromObjectString("""{"langtags": ["de", "en"], "type": "info"}""")
    val infoAnnotation = Json.fromObjectString("""{"type": "info", "value": "this is a comment"}""")

    for {
      _ <- createTestTable()

      uuid1 <- sendRequest("POST", s"/tables/1/columns/2/rows/1/annotations", langtagInfoAnnotation).map(toUuid)
      uuid2 <- sendRequest("POST", s"/tables/1/columns/1/rows/1/annotations", infoAnnotation).map(toUuid)

      ex1 <- controller.deleteCellAnnotation(1, 1, 1, UUID.fromString(uuid1)).recover({ case ex => ex })
      ex2 <- controller.deleteCellAnnotation(1, 2, 1, UUID.fromString(uuid2)).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditCellAnnotation, ScopeTable), ex1)
      assertEquals(UnauthorizedException(EditCellAnnotation, ScopeTable), ex2)
    }
  }

  @Test
  def deleteCellAnnotationWithLangtag_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editCellAnnotation"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    val langtagInfoAnnotation = Json.fromObjectString("""{"langtags": ["de", "en"], "type": "info"}""")

    for {
      _ <- createTestTable()

      uuid1 <- sendRequest("POST", s"/tables/1/columns/2/rows/1/annotations", langtagInfoAnnotation).map(toUuid)

      _ <- controller.deleteCellAnnotation(1, 2, 1, UUID.fromString(uuid1), "en")
    } yield ()
  }

  @Test
  def deleteCellHistoryWithLangtag_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
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

    val langtagInfoAnnotation = Json.fromObjectString("""{"langtags": ["de", "en"], "type": "info"}""")

    for {
      _ <- createTestTable()

      uuid1 <- sendRequest("POST", s"/tables/1/columns/2/rows/1/annotations", langtagInfoAnnotation).map(toUuid)

      ex <- controller.deleteCellAnnotation(1, 2, 1, UUID.fromString(uuid1), "en").recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditCellAnnotation, ScopeTable), ex)
    }
  }

  @Test
  def updateRowAnnotations_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editRowAnnotation"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      _ <- controller.updateRowAnnotations(1, 1, Some(true))
    } yield ()
  }

  @Test
  def updateRowAnnotations_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
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
      ex <- controller.updateRowAnnotations(1, 1, Some(true)).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditRowAnnotation, ScopeTable), ex)
    }
  }

  @Test
  def updateRowsAnnotations_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "editRowAnnotation"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()
      _ <- controller.updateRowsAnnotations(1, Some(true))
    } yield ()
  }

  @Test
  def updateRowsAnnotations_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells": [
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
      ex <- controller.updateRowsAnnotations(1, Some(true)).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditRowAnnotation, ScopeTable), ex)
    }
  }

  @Test
  def retrieveAnnotations_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-table": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    val infoAnnotation = Json.fromObjectString("""{"type": "info", "value": "this is a comment"}""")

    for {
      _ <- createTestTable()
      _ <- sendRequest("POST", s"/tables/1/columns/1/rows/1/annotations", infoAnnotation).map(toUuid)

      _ <- controller.retrieveTableWithCellAnnotations(1)
    } yield ()
  }

  @Test
  def retrieveAnnotations_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""{}""")

    val controller = createTableauxController(roleModel)

    val infoAnnotation = Json.fromObjectString("""{"type": "info", "value": "this is a comment"}""")

    for {
      _ <- createTestTable()
      _ <- sendRequest("POST", s"/tables/1/columns/1/rows/1/annotations", infoAnnotation).map(toUuid)

      ex <- controller.retrieveTableWithCellAnnotations(1).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeTable), ex)
    }
  }

  @Test
  def retrieveAnnotationCount_onlyTableTwoIsViewable_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-table-2": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "name": ".*2"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    val infoAnnotation = Json.fromObjectString("""{"type": "info", "value": "this is a comment"}""")

    for {
      _ <- createTestTable("table1")
      _ <- createTestTable("table2")
      _ <- sendRequest("POST", s"/tables/1/columns/1/rows/1/annotations", infoAnnotation).map(toUuid)
      _ <- sendRequest("POST", s"/tables/2/columns/1/rows/1/annotations", infoAnnotation).map(toUuid)

      tables <- controller.retrieveTablesWithCellAnnotationCount().map(_.getJson.getJsonArray("tables"))
    } yield {
      assertEquals(1, tables.size())
    }
  }

  @Test
  def retrieveAnnotationCount_notEvenOneTableIsViewable(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""{}""")
    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()

      tables <- controller.retrieveTablesWithCellAnnotationCount().map(_.getJson.getJsonArray("tables"))
    } yield {
      assertEquals(0, tables.size())
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_uniqueValues extends TableauxControllerAuthTest {

  /**
    * 1. column -> shorttext | single language
    * 2. column -> shorttext | multi language
    */
  def createTestTableWithShortTextColumns() = {
    createSimpleTableWithValues(
      "table",
      List(ShortTextCol("shorttext"), Multilanguage(ShortTextCol("multilanguage_shorttext"))),
      List(
        List("test1", Json.obj("de" -> "test1-de", "en" -> "test1-en")),
        List("test2", Json.obj("de" -> "test2-de", "en" -> "test2-en"))
      )
    )
  }

  @Test
  def retrieveColumnValues_authorized_ok(implicit c: TestContext): Unit = okTest {
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
      _ <- createTestTableWithShortTextColumns()
      _ <- controller.retrieveColumnValues(1, 1, None)
      _ <- controller.retrieveColumnValues(1, 2, Some("de"))
    } yield ()
  }

  @Test
  def retrieveColumnValues_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val controller = createTableauxController()

    for {
      _ <- createTestTableWithShortTextColumns()
      ex1 <- controller.retrieveColumnValues(1, 1, None).recover({ case ex => ex })
      ex2 <- controller.retrieveColumnValues(1, 2, Some("de")).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex1)
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex2)
    }
  }

  @Test
  def retrieveColumnValues_notAuthorized_table_throwsException(implicit c: TestContext): Unit = okTest {
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
      _ <- createTestTableWithShortTextColumns()
      ex <- controller.retrieveColumnValues(1, 1, None).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeTable), ex)
    }
  }

  @Test
  def retrieveColumnValues_onlyColumn2IsAllowed(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["viewCellValue"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "column": {
                                    |          "id": "2"
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
      _ <- createTestTableWithShortTextColumns()
      ex <- controller.retrieveColumnValues(1, 1, None).recover({ case ex => ex })
      _ <- controller.retrieveColumnValues(1, 2, Some("de"))
      _ <- controller.retrieveColumnValues(1, 2, Some("en"))
    } yield {
      assertEquals(UnauthorizedException(ViewCellValue, ScopeColumn), ex)
    }
  }

}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_linkCell extends LinkTestBase with TableauxControllerAuthTest {

  val putTwoLinks = Json.obj("value" -> Json.obj("values" -> Json.arr(1, 2)))

  @Test
  def updateCellLinkOrder_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editCellValue", "viewCellValue"],
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
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      _ <- controller.updateCellLinkOrder(1, 3, 1, 1, LocationEnd)
    } yield ()
  }

  @Test
  def updateCellLinkOrder_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val controller = createTableauxController()

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      ex <- controller.updateCellLinkOrder(1, 3, 1, 1, LocationEnd).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex)
    }
  }

  @Test
  def deleteLinkOfCell_authorized_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editCellValue", "viewCellValue"],
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
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      _ <- controller.deleteLink(1, 3, 1, 1)
      _ <- controller.deleteLink(1, 3, 1, 2)
    } yield ()
  }

  @Test
  def deleteLinkOfCellIfOnlyFirstTableIsViewable_authorized_ok(implicit c: TestContext): Unit = okTest {

    /* History feature and recursive requests like delete row with delete cascade could
       trigger retrieve* methods that are perhaps not allowed for a user. In this case we must mark these
       requests as internal so they are always granted.
     */

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editCellValue", "viewCellValue"],
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

    val controller = createTableauxController(roleModel)

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      _ <- controller.deleteLink(1, 3, 1, 1)
      _ <- controller.deleteLink(1, 3, 1, 2)
    } yield ()
  }

  @Test
  def deleteLinkOfCell_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {
    val controller = createTableauxController()

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      ex1 <- controller.deleteLink(1, 3, 1, 1).recover({ case ex => ex })
      ex2 <- controller.deleteLink(1, 3, 1, 2).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex1)
      assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex2)
    }
  }

  @Test
  def retrieveDependentRows_authorized_ok(implicit c: TestContext): Unit = okTest {
    // Dependent rows must be retrievable, even if the action view is not granted for dependent tables, columns, and cells.
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "id": "2"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      _ <- controller.retrieveDependentRows(2, 1)
      _ <- controller.retrieveDependentRows(2, 2)
    } yield ()
  }

  @Test
  def retrieveDependentRows_targetTableIsNotAuthorized_throwsException(implicit c: TestContext): Unit =
    okTest {
      // Dependent rows must be retrievable, even if the action view is not granted for dependent tables, columns, and cells.
      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-all-cells": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "id": "1"
                                      |        }
                                      |      }
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      for {
        linkColumnId <- setupTwoTablesWithEmptyLinks()
        _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

        ex1 <- controller.retrieveDependentRows(2, 1).recover({ case ex => ex })
        ex2 <- controller.retrieveDependentRows(2, 2).recover({ case ex => ex })
      } yield {
        assertEquals(UnauthorizedException(View, ScopeTable), ex1)
        assertEquals(UnauthorizedException(View, ScopeTable), ex2)
      }
    }

  @Test
  def retrieveRowsOfLinkCell_table1IsAuthorized_ok_table2IsNotAuthorized_throwsException(
      implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-table-1": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view"],
                                    |      "scope": "table",
                                    |      "condition": {
                                    |        "table": {
                                    |          "id": "1"
                                    |        }
                                    |      }
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      linkColumnId <- setupTwoTablesWithEmptyLinks()
      _ <- sendRequest("PUT", s"/tables/1/columns/$linkColumnId/rows/1", putTwoLinks)

      _ <- controller.retrieveForeignRows(1, linkColumnId, 1, Pagination(None, None))
      ex <- controller.retrieveForeignRows(2, linkColumnId, 1, Pagination(None, None)).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeTable), ex)
    }
  }
}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_attachmentCell extends MediaTestBase with TableauxControllerAuthTest {

  def createColumnWithLinkedAttachmentInFirstRow(tableId: TableId)(implicit c: TestContext) = {
    val columns = RequestCreation
      .Columns()
      .add(RequestCreation.AttachmentCol("Downloads"))
      .getJson

    val file = "/com/campudus/tableaux/uploads/Scr$en Shot.pdf"
    val mimetype = "application/pdf"
    val putFile = Json.obj("title" -> Json.obj("de" -> "Test PDF"))

    for {
      columnId <- sendRequest("POST", s"/tables/$tableId/columns", columns)
        .map(_.getJsonArray("columns").get[JsonObject](0).getInteger("id"))

      fileUuid <- createFile("de", file, mimetype, None) map (_.getString("uuid"))
      _ <- sendRequest("PUT", s"/files/$fileUuid", putFile)

      // Add attachments
      _ <- sendRequest("PATCH",
                       s"/tables/$tableId/columns/$columnId/rows/1",
                       Json.obj("value" -> Json.obj("uuid" -> fileUuid, "ordering" -> 1)))
    } yield (columnId.toLong, fileUuid)
  }

  @Test
  def deleteAttachmentOfCell_authorized_ok(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-edit-all-cells": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["editCellValue", "viewCellValue"],
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
      (columnId, fileUuid) <- createColumnWithLinkedAttachmentInFirstRow(tableId)

      _ <- controller.deleteAttachment(1, columnId, 1, fileUuid)
    } yield ()
  }

  @Test
  def deleteAttachmentOfCell_notAuthorized_throwsException(implicit c: TestContext): Unit = okTest {

    val controller = createTableauxController()

    for {
      tableId <- createDefaultTable()
      (columnId, fileUuid) <- createColumnWithLinkedAttachmentInFirstRow(tableId)

      ex <- controller.deleteAttachment(1, columnId, 1, fileUuid).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(EditCellValue, ScopeColumn), ex)
    }
  }

}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_translation extends TableauxControllerAuthTest {

  @Test
  def retrieveTranslationStatus_forAllTables_ok(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-only-table": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table"
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      val annotation =
        Json.fromObjectString("""{"langtags": ["en-GB"], "type": "flag", "value": "needs_translation"}""")

      for {
        _ <- createTestTable("table1")
        _ <- createTestTable("table2")
        _ <- sendRequest("POST", s"/tables/1/columns/2/rows/1/annotations", annotation)
        _ <- sendRequest("POST", s"/tables/1/columns/2/rows/2/annotations", annotation)

        translationStatus <- controller.retrieveTranslationStatus()
      } yield {
        val globalTranslationStatus = Json.obj("de-DE" -> 1.0, "en-GB" -> 0.75)
        val table1TranslationStatus = Json.obj("de-DE" -> 1.0, "en-GB" -> 0.5)
        val table2TranslationStatus = Json.obj("de-DE" -> 1.0, "en-GB" -> 1.0)

        val tablesTranslationStatus: JsonArray = translationStatus.getJson.getJsonArray("tables")

        assertJSONEquals(globalTranslationStatus, translationStatus.getJson.getJsonObject("translationStatus"))
        assertEquals(2, tablesTranslationStatus.size())

        assertJSONEquals(table1TranslationStatus,
                         tablesTranslationStatus.getJsonObject(0).getJsonObject("translationStatus"))
        assertJSONEquals(table2TranslationStatus,
                         tablesTranslationStatus.getJsonObject(1).getJsonObject("translationStatus"))
      }
    }

  @Test
  def retrieveTranslationStatus_forAllTablesEvenIfViewingIsNotGrantedForTable2_ok(implicit c: TestContext): Unit =
    okTest {

      val roleModel = initRoleModel("""
                                      |{
                                      |  "view-only-table-2": [
                                      |    {
                                      |      "type": "grant",
                                      |      "action": ["view"],
                                      |      "scope": "table",
                                      |      "condition": {
                                      |        "table": {
                                      |          "id": "2"
                                      |        }
                                      |      }
                                      |    }
                                      |  ]
                                      |}""".stripMargin)

      val controller = createTableauxController(roleModel)

      val annotation =
        Json.fromObjectString("""{"langtags": ["en-GB"], "type": "flag", "value": "needs_translation"}""")

      for {
        _ <- createTestTable("table1")
        _ <- createTestTable("table2")
        _ <- sendRequest("POST", s"/tables/1/columns/2/rows/1/annotations", annotation)
        _ <- sendRequest("POST", s"/tables/1/columns/2/rows/2/annotations", annotation)

        translationStatus <- controller.retrieveTranslationStatus()
      } yield {
        val globalTranslationStatus = Json.obj("de-DE" -> 1.0, "en-GB" -> 0.75)
        val table2TranslationStatus = Json.obj("de-DE" -> 1.0, "en-GB" -> 1.0)

        val tablesTranslationStatus: JsonArray = translationStatus.getJson.getJsonArray("tables")

        assertJSONEquals(globalTranslationStatus, translationStatus.getJson.getJsonObject("translationStatus"))
        assertEquals(1, tablesTranslationStatus.size())

        assertJSONEquals(table2TranslationStatus,
                         tablesTranslationStatus.getJsonObject(0).getJsonObject("translationStatus"))
      }
    }

}

@RunWith(classOf[VertxUnitRunner])
class TableauxControllerAuthTest_completeTable extends TableauxControllerAuthTest {

  @Test
  def retrieveCompleteTable_tableNotViewable_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""{}""")
    val controller = createTableauxController(roleModel)

    for {
      _ <- createTestTable()

      ex <- controller.retrieveCompleteTable(1).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(View, ScopeTable), ex)
    }
  }

  @Test
  def retrieveCompleteTable_onlyMultilanguageColumnsAreViewable_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "view-cells-of-text-columns": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["view", "viewCellValue"],
                                    |      "scope": "column",
                                    |      "condition": {
                                    |        "column": {
                                    |          "multilanguage": "true"
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

      res <- controller.retrieveCompleteTable(1)
    } yield {
      assertEquals(2, res.columns.length)
      assertEquals(2, res.rowList.rows.head.values.length)
    }
  }

  val createCompleteTableJson = Json.obj(
    "columns" -> Json.arr(Json.obj("kind" -> "text", "name" -> "Test Column 1"),
                          Json.obj("kind" -> "numeric", "name" -> "Test Column 2")),
    "rows" -> Json.arr(Json.obj("values" -> Json.arr("Test Field 1", 1)),
                       Json.obj("values" -> Json.arr("Test Field 2", 2)))
  )

  val expectedJson = Json.obj(
    "id" -> 1,
    "columns" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2)),
    "rows" -> Json.arr(Json.obj("id" -> 1), Json.obj("id" -> 2))
  )

  val createColumns = toCreateColumnSeq(createCompleteTableJson)
  val rowValues = toRowValueSeq(createCompleteTableJson)

  @Test
  def createCompleteTable_createTableNotAuthorized_throwsException(implicit c: TestContext): Unit = okTest {

    val controller = createTableauxController()

    for {
      ex <- controller.createCompleteTable("test table", createColumns, rowValues).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(Create, ScopeTable), ex)
    }
  }

  @Test
  def createCompleteTable_createColumnsNotAuthorized_throwsException(implicit c: TestContext): Unit = okTest {

    val roleModel = initRoleModel("""
                                    |{
                                    |  "complete-table": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "table"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      ex <- controller.createCompleteTable("test table", createColumns, rowValues).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(Create, ScopeColumn), ex)
    }
  }

  @Test
  def createCompleteTable_createColumnsAuthorizedWithoutRows_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "complete-table": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- controller.createCompleteTable("test table", createColumns, Seq(Seq.empty))
    } yield ()
  }

  @Test
  def createCompleteTable_createRows_throwsException(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "complete-table": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      ex <- controller.createCompleteTable("test table", createColumns, rowValues).recover({ case ex => ex })
    } yield {
      assertEquals(UnauthorizedException(CreateRow, ScopeTable), ex)
    }
  }

  @Test
  def createCompleteTable_createRows_ok(implicit c: TestContext): Unit = okTest {
    val roleModel = initRoleModel("""
                                    |{
                                    |  "complete-table": [
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["createRow", "create", "view"],
                                    |      "scope": "table"
                                    |    },
                                    |    {
                                    |      "type": "grant",
                                    |      "action": ["create", "view"],
                                    |      "scope": "column"
                                    |    }
                                    |  ]
                                    |}""".stripMargin)

    val controller = createTableauxController(roleModel)

    for {
      _ <- controller.createCompleteTable("test table", createColumns, rowValues)
    } yield ()
  }
}
