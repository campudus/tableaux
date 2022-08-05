package com.campudus.tableaux.helper

import com.campudus.tableaux.helper.IdentifierFlattener._

import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.skyscreamer.jsonassert.{JSONAssert, JSONCompareMode}

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

  @Test
  def flatJsonObjectSeq_simple(): Unit = {
    val expected = Seq(Json.obj("de" -> "foo", "en" -> "bar"), Json.obj("de" -> "baz", "en" -> "qux"))
    val actual = flatten(Seq(Json.arr(Json.obj("de" -> "foo", "en" -> "bar"), Json.obj("de" -> "baz", "en" -> "qux"))))
    assertEquals(expected, actual)
  }

  @Test
  def flatJsonObjectSeq_nested(): Unit = {
    val expected = Seq(
      Json.obj("de" -> "foo", "en" -> "bar"),
      Json.obj("de" -> "foo", "en" -> "bar"),
      Json.obj("de" -> "baz", "en" -> "qux")
    )
    val actual = flatten(
      Seq(
        Json.arr(
          Json.obj("de" -> "foo", "en" -> "bar"),
          Json.arr(Json.obj("de" -> "foo", "en" -> "bar"), Json.obj("de" -> "baz", "en" -> "qux"))
        )
      )
    )
    assertEquals(expected, actual)
  }
}

class SinglelanguageConcatenationTest {

  @Test
  def concatenate_mixedSeq(): Unit = {
    val actual = concatenateSingleLang(Seq("Hello,", "now", "it", "is", 10, "past", 5, "O’clock"))
    assertEquals("Hello, now it is 10 past 5 O’clock", actual)
  }

  @Test
  def concatenate_withInvalidValues(): Unit = {
    val actual = concatenateSingleLang(Seq("1", null, 2, "  3  ", "    ", null, 4))
    assertEquals("1 2 3 4", actual)
  }
}

class MultilanguageConcatenationTest {

  @Test
  def concatenate_fullSuppliedLangtags(): Unit = {
    val expected =
      """
        |{
        |  "de":"foo baz",
        |  "en":"bar 42"
        |}
        |""".stripMargin

    val actual = compress(
      Seq("de", "en"),
      Seq(Json.arr(Json.obj("de" -> "foo", "en" -> "bar"), Json.obj("de" -> "baz", "en" -> 42)))
    )
    JSONAssert.assertEquals(expected, actual.getOrElse(Json.emptyObj()).toString, JSONCompareMode.STRICT)
  }

  @Test
  def concatenate_MultiLanguageAndSingleLanguage(): Unit = {
    val langtags = Seq("de", "en")
    val expected =
      """
        |{
        |  "de":"foo 42",
        |  "en":"bar 42"
        |}
        |""".stripMargin

    val flatValues = flatten(Seq(Json.arr(Json.obj("de" -> "foo", "en" -> "bar"), 42)))
    val actual = concatenateMultiLang(langtags, flatValues)
    JSONAssert.assertEquals(expected, actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def concatenate_fallbackLanguageIsDefined_FallbackToHeadLangtag(): Unit = {
    val langtags = Seq("de", "en")
    val expected =
      """
        |{
        |  "de": "foo 1337",
        |  "en": "bar 1337"
        |}
        |""".stripMargin

    val flatValues = flatten(Seq(Json.arr(Json.obj("de" -> "foo", "en" -> "bar"), Json.obj("de" -> 1337))))
    val actual = concatenateMultiLang(langtags, flatValues)
    JSONAssert.assertEquals(expected, actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def concatenate_fallbackLanguageIs_NOT_Defined_NO_FallbackToHeadLangtag(): Unit = {
    val langtags = Seq("de", "en")
    val expected =
      """
        |{
        |  "de": "foo",
        |  "en": "bar 42"
        |}
        |""".stripMargin

    val flatValues = flatten(Seq(Json.arr(Json.obj("de" -> "foo", "en" -> "bar"), Json.obj("en" -> 42))))
    val actual = concatenateMultiLang(langtags, flatValues)
    JSONAssert.assertEquals(expected, actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def concatenate_fallbackLanguageIs_NOT_DefinedAndInt(): Unit = {
    val langtags = Seq("de", "en")
    val expected =
      """
        |{
        |  "de": "1337",
        |  "en": "foo 1337"
        |}
        |""".stripMargin

    val actual = concatenateMultiLang(langtags, Seq(Json.obj("en" -> "foo"), 1337))
    JSONAssert.assertEquals(expected, actual.toString, JSONCompareMode.STRICT)
  }

  @Test
  def concatenate_filterInvalidValues(): Unit = {
    val langtags = Seq("de", "en")
    val expected =
      """
        |{
        |  "de": "1337 is super!",
        |  "en": "1337 is super!"
        |}
        |""".stripMargin

    val actual = concatenateMultiLang(langtags, Seq(Json.obj("de" -> null), 1337, Json.obj("de" -> " "), "is super!"))
    JSONAssert.assertEquals(expected, actual.toString, JSONCompareMode.STRICT)
  }

}
