package com.campudus.tableaux.testtools

import com.campudus.tableaux.database.domain.LinkAttributeDefinition

import org.junit.{After, Before}

/**
  * Mix into a test class that needs a link attribute feature which is gated off for rollout - more than one definition
  * per column, or a multilanguage one. Both gates are rollout decisions rather than structural limits (see
  * LinkAttributeDefinition), so the code behind them exists and has to keep working; without a way to lift them in a
  * test, none of it could be reached at all.
  *
  * Override only what the class actually needs - what isn't overridden stays at the production default, so a class that
  * lifts the multilanguage gate is still held to the one-definition cap. The `@After` puts both back: the gates are
  * global mutable state, so a class that left one lifted would silently lift it for every test class running after it
  * in the same JVM.
  */
trait LinkAttributeTestOverrides {

  protected def testLinkAttributeMaxCount: Int = LinkAttributeDefinition.defaultMaxCount

  protected def testMultilanguageLinkAttributesSupported: Boolean =
    LinkAttributeDefinition.defaultMultilanguageSupported

  @Before
  def applyLinkAttributeOverrides(): Unit = {
    LinkAttributeDefinition.setMaxCountForTest(testLinkAttributeMaxCount)
    LinkAttributeDefinition.setMultilanguageSupportedForTest(testMultilanguageLinkAttributesSupported)
  }

  @After
  def resetLinkAttributeOverrides(): Unit = {
    LinkAttributeDefinition.resetMaxCountForTest()
    LinkAttributeDefinition.resetMultilanguageSupportedForTest()
  }
}
