package com.campudus.tableaux.database

import java.util.UUID
import org.junit.{Assert, Test}
import org.junit.Assert.assertEquals
import org.scalatest.Assertions._

class LocationTypeTest {

  @Test
  def testLocationTypeApply_withLong(): Unit = {
    assertEquals(LocationStart, LocationType("start", None))
    assertEquals(LocationEnd, LocationType("end", None))
    assertEquals(LocationBefore(10L), LocationType("before", Option(10L)))
    assertEquals(LocationBefore(10L), LocationType("before", Option(10)))
  }

  @Test
  def testLocationTypeApply_withValidUUID(): Unit = {
    val uuidString = "123e4567-e89b-12d3-a456-426614174000"
    assertEquals(LocationStart, LocationType("start", None))
    assertEquals(LocationEnd, LocationType("end", None))
    assertEquals(
      LocationBefore(UUID.fromString(uuidString)),
      LocationType("before", Option(UUID.fromString(uuidString)))
    )
  }

  @Test
  def testLocationTypeApply_withInvalidUUID(): Unit = {
    val uuidString = "XY123"
    assertEquals(LocationStart, LocationType("start", None))
    assertEquals(LocationEnd, LocationType("end", None))
    assertThrows[IllegalArgumentException] {
      LocationType("before", Option(UUID.fromString(uuidString)))
    }
  }

  @Test
  def testLocationTypeApplyException(): Unit = {
    var exceptionCounter = 0
    val invalid = "Invalid location and/or relativeTo id."

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
