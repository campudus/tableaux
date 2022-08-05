package com.campudus.tableaux.database.domain

import com.campudus.tableaux._

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Matchers._

class PaginationTest {

  @Test
  def checkOffset_valueGreaterMinusOne_returnsOk(): Unit = {
    val p = Pagination(Option(1), None)
    assertEquals(OkArg(p), p.check)
  }

  @Test
  def checkOffset_negativeValue_returnsFailedArg(): Unit = {
    val p = Pagination(Option(-1), None)
    p.check shouldBe a[FailArg[_]]
  }

  @Test
  def checkLimit_valueGreaterMinusOne_returnsOk(): Unit = {
    val p = Pagination(None, Option(1))
    assertEquals(OkArg(p), p.check)
  }

  @Test
  def checkLimit_negativeValue_returnsFailedArg(): Unit = {
    val p = Pagination(None, Option(-1))
    p.check shouldBe a[FailArg[_]]
  }

  @Test
  def checkLimit_checkOffset_negativeValue_returnsFailedArg(): Unit = {
    val p = Pagination(Option(-1), Option(-1))
    p.check shouldBe a[FailArg[_]]
  }
}
