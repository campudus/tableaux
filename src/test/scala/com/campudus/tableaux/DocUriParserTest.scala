package com.campudus.tableaux

import com.campudus.tableaux.helper.DocUriParser
import org.junit.Assert._
import org.junit.Test

class DocUriParserTest {

  @Test
  def checkSimpleUri(): Unit = {
    assertEquals(
      ("http", "localhost:8181", "some/test"),
      DocUriParser.parse("http://localhost:8181/some/test/docs/index.html")
    )
  }

  @Test
  def checkUriWithoutRest(): Unit = {
    assertEquals(
      ("http", "localhost:8181", ""),
      DocUriParser.parse("http://localhost:8181/docs/index.html")
    )
  }

  @Test
  def checkHttps(): Unit = {
    assertEquals(
      ("https", "localhost:8181", ""),
      DocUriParser.parse("https://localhost:8181/docs/index.html")
    )
  }

  @Test
  def checkUriWithSinglePath(): Unit = {
    assertEquals(
      ("https", "localhost:8181", "something"),
      DocUriParser.parse("https://localhost:8181/something/docs")
    )
  }

  @Test
  def checkUriWithDocsInPath(): Unit = {
    assertEquals(
      ("https", "localhost:8181", "something/docs/whatever"),
      DocUriParser.parse("https://localhost:8181/something/docs/whatever/docs")
    )
  }

  @Test
  def checkHostWithoutPort(): Unit = {
    assertEquals(
      ("https", "localhost", "something/docs/whatever"),
      DocUriParser.parse("https://localhost/something/docs/whatever/docs")
    )
  }

  @Test
  def checkOtherHostsWithoutPort(): Unit = {
    assertEquals(
      ("https", "campudus.com", "something/docs/whatever"),
      DocUriParser.parse("https://campudus.com/something/docs/whatever/docs")
    )
  }

  @Test
  def checkSubdomainHostsWithoutPort(): Unit = {
    assertEquals(
      ("https", "tableaux.campudus.com", "something/docs/whatever"),
      DocUriParser.parse("https://tableaux.campudus.com/something/docs/whatever/docs")
    )
  }

  @Test
  def checkHostWithDifferentPort(): Unit = {
    assertEquals(
      ("https", "tableaux.campudus.com:8081", "something/docs/whatever"),
      DocUriParser.parse("https://tableaux.campudus.com:8081/something/docs/whatever/docs")
    )
  }

  @Test
  def noDocsShowsDefaultValues(): Unit = {
    assertEquals(
      ("http", "localhost:8181", ""),
      DocUriParser.parse("https://tableaux.campudus.com:8081/something/whatever/without/d.o.c.s.")
    )
  }

}
