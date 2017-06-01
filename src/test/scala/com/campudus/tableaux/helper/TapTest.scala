package com.campudus.tableaux.helper

import org.junit.Assert.{assertEquals, assertNotSame}
import org.junit.Test

class TapTest {

  @Test
  def testImplicitStringTap(): Unit = {
    import Tap.anyToTap

    assertEquals(
      "lustig",
      "lustig".tap(input => assertEquals("lustig", input))
    )
  }

  @Test
  def testImplicitCaseClassTap(): Unit = {
    import Tap.anyToTap

    case class Test(a: String)

    assertEquals(
      Test("b"),
      Test("b").tap(input => assertEquals(Test("b"), input))
    )

    assertEquals(
      Test("a"),
      Test("a").tap(_ => Test("b"))
    )

    assertNotSame(
      Test("a"),
      Test("b").tap(_ => Test("a"))
    )
  }
}
