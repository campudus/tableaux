package com.campudus.tableaux

import org.junit.Test
import org.junit.Assert._
import com.campudus.tableaux.ArgumentChecker._

class ArgumentCheckerTest {

  @Test
  def checkValidNotNull(): Unit = {
    assertEquals(OkArg(123), notNull(123, ""))
    assertEquals(OkArg("abc"), notNull("abc", ""))
    assertEquals(OkArg(""), notNull("", ""))
    assertEquals(OkArg(0), notNull(0, ""))
    assertEquals(OkArg(Nil), notNull(Nil, ""))
  }

  @Test
  def checkInvalidNotNull(): Unit = {
    assertEquals(FailArg(InvalidJsonException("Warning: test is null", "null")), notNull(null, "test"))
  }

  @Test
  def checkValidGreaterZero(): Unit = {
    assertEquals(OkArg(123), greaterZero(123))
    assertEquals(OkArg(1), greaterZero(1))
    assertEquals(OkArg(Long.MaxValue), greaterZero(Long.MaxValue))
  }

  @Test
  def checkInvalidGreaterZero(): Unit = {
    assertEquals(FailArg(InvalidJsonException("Argument -1 is not greater than zero", "invalid")), greaterZero(-1))
    assertEquals(FailArg(InvalidJsonException("Argument 0 is not greater than zero", "invalid")), greaterZero(0))
    assertEquals(FailArg(InvalidJsonException(s"Argument ${Long.MinValue} is not greater than zero", "invalid")), greaterZero(Long.MinValue))
  }

  @Test
  def checkValidNonEmpty(): Unit = {
    assertEquals(OkArg(Seq(123)), nonEmpty(Seq(123), "test"))
    assertEquals(OkArg(Seq("abc")), nonEmpty(Seq("abc"), "test"))
    assertEquals(OkArg(Seq("")), nonEmpty(Seq(""), "test"))
    assertEquals(OkArg(Seq(Seq(123))), nonEmpty(Seq(Seq(123)), "test"))
    assertEquals(OkArg(Seq(123, 123)), nonEmpty(Seq(123, 123), "test"))
    assertEquals(OkArg(Seq(Seq(123), Seq(123))), nonEmpty(Seq(Seq(123), Seq(123)), "test"))
  }

  @Test
  def checkInvalidNonEmpty(): Unit = {
    assertEquals(FailArg(InvalidJsonException("Warning: test is empty.", "empty")), nonEmpty(Seq(), "test"))
  }

  @Test
  def checkValidArguments(): Unit = {
    checkArguments(notNull(123, "test"), greaterZero(1), greaterZero(2), notNull("foo", "test"), nonEmpty(Seq(123), "test"))
  }

  @Test
  def checkInvalidArguments(): Unit = {
    try {
      checkArguments(notNull(null, "test"), greaterZero(1), greaterZero(-4), notNull("foo", "test"))
      fail("Should throw an exception")
    } catch {
      case ex: IllegalArgumentException =>
        assertEquals("(0) Warning: test is null\n(2) Argument -4 is not greater than zero", ex.getMessage)
      case _: Throwable => fail("Should throw an IllegalArgumentException")
    }
  }

}
