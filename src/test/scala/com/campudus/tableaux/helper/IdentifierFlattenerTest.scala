package com.campudus.tableaux.helper

import com.campudus.tableaux.helper.IdentifierFlattener._
import org.junit.Test
import org.vertx.scala.core.json.{Json, JsonObject}
import org.junit.Assert._

class IsMultiLanguageValueTest {

  @Test
  def containsMultiLanguageValue_String(): Unit = {
    assertFalse(isMultiLanguageValue("foo"))
  }

  @Test
  def containsMultiLanguageValue_Integer(): Unit = {
    assertFalse(isMultiLanguageValue(1))
  }

  @Test
  def containsMultiLanguageValue_JsonObject(): Unit = {
    assertTrue(isMultiLanguageValue(Json.obj("de" -> "value")))
  }
}

class ContainsMultiLanguageTest {

  @Test
  def containsMultiLanguageValue_seqOnlyWithIntegers(): Unit = {
    assertFalse(containsMultiLanguageValue(Seq(1, 2, 3)))
  }

  @Test
  def containsMultiLanguageValue_seqOnlyWithStrings(): Unit = {
    assertFalse(containsMultiLanguageValue(Seq("foo", "bar", "baz")))
  }

  @Test
  def containsMultiLanguageValue_seqWithMixedIntegersAndStrings(): Unit = {
    assertFalse(containsMultiLanguageValue(Seq("foo", 1, "bar", 42, "baz")))
  }

  @Test
  def containsMultiLanguageValue_seqWithSingleJsonObject(): Unit = {
    assertTrue(containsMultiLanguageValue(Seq(Json.obj("de" -> "value"))))
  }

  @Test
  def containsMultiLanguageValue_seqWithOneJsonObject(): Unit = {
    assertTrue(containsMultiLanguageValue(Seq(1, 2, Json.obj("de" -> "value"))))
  }

  @Test
  def containsMultiLanguageValue_seqWithMultipleJsonObjects(): Unit = {
    assertTrue(containsMultiLanguageValue(Seq(Json.obj("de" -> "foo"), "bar", Json.obj("de" -> "baz"))))
  }
}

class FlattenTest {

  @Test
  def flatten_seqOfIntegers(): Unit = {
    val actual = flatten(Seq(1, 2, 3))
    assertEquals(Seq(1, 2, 3), actual)
  }

  @Test
  def flatten_seqOfStrings(): Unit = {
    val actual = flatten(Seq("hello", "world", "!"))
    assertEquals(Seq("hello", "world", "!"), actual)
  }

  @Test
  def flatten_seqOfMixedTypes(): Unit = {
    val actual = flatten(Seq(2, "or", 3, "wishes"))
    assertEquals(Seq(2, "or", 3, "wishes"), actual)
  }

  @Test
  def flatten_simpleValue(): Unit = {
    val actual = flatten("a string")
    assertEquals(Seq("a string"), actual)
  }

  @Test
  def flatten_seqOfNestedIntegerSequences(): Unit = {
    val actual = flatten(Seq(1, Seq(2, Seq(3, 4), 5), 6))
    assertEquals(Seq(1, 2, 3, 4, 5, 6), actual)
  }

  @Test
  def flatten_seqOfNestedMixedTypeSequences(): Unit = {
    val actual = flatten(Seq("Hello", Seq("now", "it", Seq("is", 10), "past", 5), "O’clock"))
    assertEquals(Seq("Hello", "now", "it", "is", 10, "past", 5, "O’clock"), actual)
  }
}
