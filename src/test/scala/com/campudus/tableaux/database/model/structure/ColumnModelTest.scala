package com.campudus.tableaux.database.model.structure

import com.campudus.tableaux.RequestContext
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain.{BasicColumnInformation, CreateSimpleColumn, SimpleValueColumn, Table}
import com.campudus.tableaux.database.model.structure.ColumnModel.isColumnGroupMatchingToFormatPattern
import org.junit.Assert._
import org.junit.{Before, Test}

class ColumnModelTest {

  implicit val requestContext = RequestContext()

  var col1: SimpleValueColumn[_] = _
  var col2: SimpleValueColumn[_] = _

  @Before
  def setup(): Unit = {
    val sc1 = CreateSimpleColumn("c1", null, null, LanguageNeutral, identifier = false, Nil, separator = false)
    val sc2 = CreateSimpleColumn("c2", null, null, LanguageNeutral, identifier = false, Nil, separator = false)

    val testTable = Table(1, "table", hidden = false, null, null, null, null, None)
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
//             | [2, 1]  | "{{1}} mm × {{2}} mm" | [x]     |
//             | [1, 2]  | "{{2}} mm × {{1}} mm" | [x]     |

  @Test
  def isColumnGroupMatchingToFormatPattern_oneColumn_oneWildcard_isValid(): Unit = {
    val formatPattern = "{{1}} mm"
    val columns = Seq(col1)

    assertTrue(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_twoColumns_twoWildcards_isValid(): Unit = {
    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col1, col2)

    assertTrue(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_oneColumn_oneUniqueWildcards_isValid(): Unit = {
    val formatPattern = "{{1}} mm × {{1}} mm"
    val columns = Seq(col1)

    assertTrue(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_oneColumn_twoWildcards_isNotValid(): Unit = {
    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col1)

    assertFalse(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_twoColumns_oneWildcards_isNotValid(): Unit = {
    val formatPattern = "{{1}} mm"
    val columns = Seq(col1, col2)

    assertFalse(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_oneColumn_invalidWildcardNumber_isNotValid(): Unit = {
    val formatPattern = "{{42}} mm"
    val columns = Seq(col1)

    assertFalse(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_oneColumn_None_isValid(): Unit = {
    val columns = Seq(col1)

    assertTrue(isColumnGroupMatchingToFormatPattern(None, columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_oneColumn_invalidPattern_isNotValid(): Unit = {
    val formatPattern = "{{a}} mm"
    val columns = Seq(col1)

    assertFalse(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_oneHigherColumnId_patternWithHigherWildcard_isValid(): Unit = {
    val sc42 =
      CreateSimpleColumn("c42", null, null, LanguageNeutral, identifier = false, List(), separator = false)
    val testTable = Table(1, "table", hidden = false, null, null, null, null, None)
    val bci42 = BasicColumnInformation(testTable, 42, 1, null, sc42)

    val col42 = SimpleValueColumn(ShortTextType, LanguageNeutral, bci42)

    val formatPattern = "{{42}} mm"
    val columns = Seq(col42)

    assertTrue(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_twoColumnsInReverseOrder_twoWildcards_isValid(): Unit = {
    val formatPattern = "{{1}} mm × {{2}} mm"
    val columns = Seq(col2, col1)

    assertTrue(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }

  @Test
  def isColumnGroupMatchingToFormatPattern_twoColumns_twoWildcardsInReverseOrder_isValid(): Unit = {
    val formatPattern = "{{2}} mm × {{1}} mm"
    val columns = Seq(col1, col2)

    assertTrue(isColumnGroupMatchingToFormatPattern(Some(formatPattern), columns))
  }
}
