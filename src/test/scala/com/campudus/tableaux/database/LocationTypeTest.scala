package com.campudus.tableaux.database

import org.junit.Assert.assertEquals
import org.junit.{Assert, Test}

class LocationTypeTest {

  @Test
  def testLocationTypeApply(): Unit = {
    assertEquals(LocationStart, LocationType("start", None))
    assertEquals(LocationEnd, LocationType("end", None))
    assertEquals(LocationBefore(10l), LocationType("before", Option(10l)))
    assertEquals(LocationBefore(10l), LocationType("before", Option(10)))
  }

  @Test
  def testLocationTypeApplyException(): Unit = {
    var exceptionCounter = 0
    val invalid = "Invalid location and/or relativeTo row id."

    def exceptionTest(a: => Unit): Unit = {
      try {
        a
        Assert.fail("should fail")
      } catch {
        case ex: IllegalArgumentException =>
          exceptionCounter += 1

          assertEquals(invalid, ex.getMessage)

        case _: Throwable =>
          Assert.fail("should fail with an IllegalArgumentException")
      }
    }

    exceptionTest(LocationType("invalid", Option(1)))
    exceptionTest(LocationType("start", Option(1)))
    exceptionTest(LocationType("end", Option(1)))
    exceptionTest(LocationType("Before", None))

    assertEquals(4, exceptionCounter)
  }
}
