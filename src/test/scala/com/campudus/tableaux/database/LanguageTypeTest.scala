package com.campudus.tableaux.database

import org.junit.Assert.assertEquals
import org.junit.{Assert, Test}

class LanguageTypeTest {

  @Test
  def testLanguageTypeApply(): Unit = {
    assertEquals(LanguageNeutral, LanguageType(None))
    assertEquals(LanguageNeutral, LanguageType(Some("neutral")))
    assertEquals(MultiLanguage, LanguageType(Some("language")))
    assertEquals(MultiCountry(CountryCodes(Seq.empty)), LanguageType(Some("country")))
  }

  @Test
  def testLanguageTypeApplyException(): Unit = {
    var exceptionCounter = 0
    val invalid = "Invalid argument for LanguageType.apply"

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

    exceptionTest(LanguageType(Some("invalid")))

    assertEquals(1, exceptionCounter)
  }

  @Test
  def testLanguageTypeToString(): Unit = {
    assertEquals("neutral", LanguageNeutral.toString)
    assertEquals("language", MultiLanguage.toString)
    assertEquals("country", MultiCountry(CountryCodes(Seq.empty)).toString)
  }

  @Test
  def testMultiCountryUnapply(): Unit = {
    val languageNeutral = LanguageNeutral
    val multiCountry = MultiCountry(CountryCodes(Seq("DE", "AT", "GB")))

    multiCountry match {
      case MultiCountry(countryCodes) =>
        assertEquals(Seq("DE", "AT", "GB"), countryCodes.codes)
      case _ =>
        Assert.fail("should match with MultiCountry")
    }

    languageNeutral match {
      case MultiCountry(_) =>
        Assert.fail("should not match with MultiCountry")
      case _ =>
      // Jep!
    }
  }
}
