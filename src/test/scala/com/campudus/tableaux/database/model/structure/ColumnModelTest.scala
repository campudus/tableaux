package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{BasicColumnInformation, CreateSimpleColumn, SimpleValueColumn, Table}
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

class ColumnModelTest {

  val sc1 = CreateSimpleColumn("c1", null, null, LanguageNeutral, identifier = false, frontendReadOnly = false, List())
  val sc2 = CreateSimpleColumn("c2", null, null, LanguageNeutral, identifier = false, frontendReadOnly = false, List())

  val testTable = Table(1, "table", false, null, null, null, null)
  val bci1 = BasicColumnInformation(testTable, 1, 1, null, sc1)
  val bci2 = BasicColumnInformation(testTable, 1, 1, null, sc2)

  val col1 = SimpleValueColumn(ShortTextType, LanguageNeutral, bci1)
  val col2 = SimpleValueColumn(ShortTextType, LanguageNeutral, bci2)

//             | Columns |        Format         | isValid |
//             | ------- | --------------------- | ------- |
//             | [1]     | "{{1}} mm"            | [x]     |
//             | [1, 2]  | "{{1}} mm × {{2}} mm" | [x]     |
//             | [1]     | "{{1}} mm × {{1}} mm" | [x]     |
//             | [1]     | "{{1}} mm × {{2}} mm" | [ ]     |
//             | [1, 2]  | "{{1}} mm"            | [ ]     |
//             | [1]     | "{{42}} mm"           | [ ]     |
//             | [1]     | None                  | [x]     |

  @Test
  def groupingMatchesToFormatPattern_oneColumn_oneWildcard_isValid(): Unit = {

    val formatPattern = "{{1}} mm"
    val columns = Seq(col1)

    assertTrue(ColumnModel.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_twoColumns_twoWildcards_isValid(): Unit = {

    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col1, col2)

    assertTrue(ColumnModel.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_oneUniqueWildcards_isValid(): Unit = {

    val formatPattern = "{{1}} mm × {{1}} mm"
    val columns = Seq(col1)

    assertTrue(ColumnModel.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_twoWildcards_isNotValid(): Unit = {

    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col1)

    assertFalse(ColumnModel.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_twoColumns_oneWildcards_isNotValid(): Unit = {

    val formatPattern = "{{1}} mm"
    val columns = Seq(col1, col2)

    assertFalse(ColumnModel.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_invalidWildcardNumber_isNotValid(): Unit = {

    val formatPattern = "{{42}} mm"
    val columns = Seq(col1)

    assertFalse(ColumnModel.groupingMatchesToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def groupingMatchesToFormatPattern_oneColumn_None_isValid(): Unit = {

    val columns = Seq(col1)

    assertTrue(ColumnModel.groupingMatchesToFormatPattern(None, columns))
  }
}
