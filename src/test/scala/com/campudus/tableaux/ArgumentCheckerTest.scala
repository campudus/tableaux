package com.campudus.tableaux

import org.junit.Test
import org.junit.Assert._
import com.campudus.tableaux.ArgumentChecker._

class ArgumentCheckerTest {

  @Test
  def checkValidNotNull(): Unit = {
    assertEquals(OkArg, notNull(123))
    assertEquals(OkArg, notNull("abc"))
    assertEquals(OkArg, notNull(""))
    assertEquals(OkArg, notNull(0))
    assertEquals(OkArg, notNull(Nil))
  }

  @Test
  def checkInvalidNotNull(): Unit = {
    assertEquals(FailArg("Argument is null"), notNull(null))
  }

  @Test
  def checkValidGreaterZero(): Unit = {
    assertEquals(OkArg, greaterZero(123))
    assertEquals(OkArg, greaterZero(1))
    assertEquals(OkArg, greaterZero(Long.MaxValue))
  }

  @Test
  def checkInvalidGreaterZero(): Unit = {
    assertEquals(FailArg("Argument -1 is not greater than zero"), greaterZero(-1))
    assertEquals(FailArg("Argument 0 is not greater than zero"), greaterZero(0))
    assertEquals(FailArg(s"Argument ${Long.MinValue} is not greater than zero"), greaterZero(Long.MinValue))
  }

  @Test
  def checkValidArguments(): Unit = {
    checkArguments(notNull(123), greaterZero(1), greaterZero(2), notNull("foo"))
  }

  @Test
  def checkInvalidArguments(): Unit = {
    try {
      checkArguments(notNull(null), greaterZero(1), greaterZero(-4), notNull("foo"))
      fail("Should throw an exception")
    } catch {
      case ex: IllegalArgumentException =>
        assertEquals("(0) Argument is null\n(2) Argument -4 is not greater than zero", ex.getMessage)
      case _: Throwable => fail("Should throw an IllegalArgumentException")
    }
  }

}
