package com.campudus.tableaux.database

import org.junit.Assert.assertEquals
import org.junit.Test

class SqlStatementTest {

  @Test
  def commandOfPlainStatement(): Unit = {
    assertEquals("ALTER", SqlStatement.commandOf("ALTER TABLE foo ADD COLUMN bar INT;"))
    assertEquals("SELECT", SqlStatement.commandOf("  \n select 1"))
  }

  @Test
  def commandOfSkipsLeadingLineComments(): Unit = {
    assertEquals("ALTER", SqlStatement.commandOf("-- a comment\nALTER TABLE foo ADD COLUMN bar INT;"))
    assertEquals("CREATE", SqlStatement.commandOf("-- one\r\n\r\n  -- two\r\nCREATE TABLE foo ();"))
  }

  @Test
  def commandOfCommentOnlyStatementIsEmpty(): Unit = {
    assertEquals("", SqlStatement.commandOf("-- nothing but a comment"))
  }

  @Test
  def withoutLeadingCommentsKeepsLaterComments(): Unit = {
    assertEquals(
      "ALTER TABLE foo ADD COLUMN bar INT; -- trailing\n",
      SqlStatement.withoutLeadingComments("-- head\nALTER TABLE foo ADD COLUMN bar INT; -- trailing\n")
    )
  }
}
