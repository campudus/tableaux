package com.campudus.tableaux.testtools

import org.junit.Assert._

trait AssertionHelpers {

  protected def checkPartsInRandomOrder(parts: Seq[Seq[Any]], longList: Seq[Any]): Unit = {
    assertEquals(parts.map(_.length).sum, longList.length)
    val indices = for {
      part <- parts
    } yield longList.indexOfSlice(part)
    assertTrue(indices.forall(_ >= 0))
    assertEquals(indices.length, indices.distinct.length)
  }

}
