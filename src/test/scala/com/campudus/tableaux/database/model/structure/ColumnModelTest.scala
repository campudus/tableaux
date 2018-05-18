package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{BasicColumnInformation, CreateSimpleColumn, SimpleValueColumn, Table}
import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.{Assert, Before, Test}

@RunWith(classOf[VertxUnitRunner])
class ColumnModelTest extends TableauxTestBase {
//  var sqlConnection: SQLConnection = _
//  var dbConnection: DatabaseConnection = _
//  var cm: ColumnModel = _

  var col1: SimpleValueColumn[_] = _
  var col2: SimpleValueColumn[_] = _

  @Before
  def setup(implicit c: TestContext): Unit = {

//    sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
//    dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)
//    cm = new ColumnModel(dbConnection)

    val sc1 =
      CreateSimpleColumn("c1", null, null, LanguageNeutral, identifier = false, frontendReadOnly = false, List())
    val sc2 =
      CreateSimpleColumn("c2", null, null, LanguageNeutral, identifier = false, frontendReadOnly = false, List())

    val testTable = Table(1, "table", false, null, null, null, null)
    val bci1 = BasicColumnInformation(testTable, 1, 1, null, sc1)
    val bci2 = BasicColumnInformation(testTable, 2, 1, null, sc2)

    col1 = SimpleValueColumn(ShortTextType, LanguageNeutral, bci1)
    col2 = SimpleValueColumn(ShortTextType, LanguageNeutral, bci2)
  }

//             | Columns |        Format         | isValid |
//             | ------- | --------------------- | ------- |
//             | [1]     | "{{1}} mm"            | [x]     |
//             | [1, 2]  | "{{1}} mm × {{2}} mm" | [x]     |
//             | [1]     | "{{1}} mm × {{1}} mm" | [x]     |
//             | [1]     | "{{1}} mm × {{2}} mm" | [ ]     |
//             | [1, 2]  | "{{1}} mm"            | [ ]     |
//             | [1]     | "{{42}} mm"           | [ ]     |
//             | [1]     | None                  | [x]     |
//             | [1]     | "{{a}} mm"            | [ ]     |
//             | [42]    | "{{42}} mm"           | [x]     |

  @Test
  def groupingMatchesToFormatPattern_oneColumn_oneWildcard_isValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{1}} mm"
    val columns = Seq(col1)

    assertTrue(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_twoColumns_twoWildcards_isValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col1, col2)

    assertTrue(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_oneUniqueWildcards_isValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{1}} mm × {{1}} mm"
    val columns = Seq(col1)

    assertTrue(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_twoWildcards_isNotValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col1)

    assertFalse(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_twoColumns_oneWildcards_isNotValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{1}} mm"
    val columns = Seq(col1, col2)

    assertFalse(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_invalidWildcardNumber_isNotValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{42}} mm"
    val columns = Seq(col1)

    assertFalse(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_None_isValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val columns = Seq(col1)

    assertTrue(cm.groupingMatchesToFormatPattern(None, columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_invalidPattern_isNotValid(implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel

    val formatPattern = "{{a}} mm"
    val columns = Seq(col1)

    assertFalse(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneHigherColumnId_patternWithHigherWildcard_isValid(
      implicit c: TestContext): Unit = {
    val cm: ColumnModel = setupColumnModel
    val sc42 = CreateSimpleColumn("c42", null, null, LanguageNeutral, false, false, List())
    val testTable = Table(1, "table", false, null, null, null, null)
    val bci42 = BasicColumnInformation(testTable, 42, 1, null, sc42)

    val col42 = SimpleValueColumn(ShortTextType, LanguageNeutral, bci42)

    val formatPattern = "{{42}} mm"
    val columns = Seq(col42)

    assertTrue(cm.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  private def setupColumnModel = {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)
    val dbConnection = DatabaseConnection(this.vertxAccess(), sqlConnection)

    new ColumnModel(dbConnection)
  }
}
