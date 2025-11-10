package com.campudus.tableaux.helper

import org.junit.Assert.{assertEquals, assertNotSame}
import org.junit.Test

class UnionTableRowHelper {

  @Test
  def calcRowId(): Unit = {
    assertEquals(1000002L, UnionTableHelper.calcRowId(1L, 2L, 1000000L))
    assertEquals(1337L, UnionTableHelper.calcRowId(13L, 37L, 100L))
    // working but low offset would cause issues on extracting tableId and rowId again
    assertEquals(401L, UnionTableHelper.calcRowId(3L, 101L, 100L))
  }

  @Test
  def extractTableIdAndRowId(): Unit = {
    val (tableId1, rowId1) = UnionTableHelper.extractTableIdAndRowId(1000002L, 1000000L)
    assertEquals(1L, tableId1)
    assertEquals(2L, rowId1)

    val (tableId2, rowId2) = UnionTableHelper.extractTableIdAndRowId(1337L, 100L)
    assertEquals(13L, tableId2)
    assertEquals(37L, rowId2)

    val (tableId3, rowId3) = UnionTableHelper.extractTableIdAndRowId(401L, 100L)
    assertEquals(4L, tableId3)
    assertEquals(1L, rowId3)

  }
}
