package com.campudus.tableaux.helper

import com.campudus.tableaux.database.model.CreateHistoryModel
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.{Assert, Test}
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class RejectNonChanges extends TableauxTestBase {

  @Test
  def rejectEqualValues(): Unit = {
    val oldMap = Map("de" -> Some("HallWrong"), "en" -> Some("Hello"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val result = JsonUtils.rejectNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("de" -> Some("Hallo")), result)
  }

  @Test
  def ignoreAdditionalValuesInOldMap(): Unit = {
    val oldMap = Map("de" -> Some("HallWrong"), "en" -> Some("Hello"), "fr" -> Some("Bonjour"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val result = JsonUtils.rejectNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("de" -> Some("Hallo")), result)
  }

  @Test
  def doNotRejectNullValuesInNewMap(): Unit = {
    val oldMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> None)
    val result = JsonUtils.rejectNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("en" -> None), result)
  }

  @Test
  def returnEmptyMapIfAllKeysAreRejected(): Unit = {
    val oldMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val result = JsonUtils.rejectNonChanges(newMap, oldMap)

    Assert.assertEquals(Map.empty[String, Option[Any]], result)
  }
}

@RunWith(classOf[VertxUnitRunner])
class MultiLangValueToMap extends TableauxTestBase {

  @Test
  def multiLangValueToMap_validMultiLangValue(): Unit = {
    val json = Json.obj(
      "de" -> "Hallo",
      "en" -> "Hello"
    )

    val result = JsonUtils.multiLangValueToMap(json)

    Assert.assertEquals(Map("de" -> Some("Hallo"), "en" -> Some("Hello")), result)
  }

  @Test
  def multiLangValueToMap_emptyValueObject(): Unit = {
    val json = Json.emptyObj()
    val result = JsonUtils.multiLangValueToMap(json)

    Assert.assertEquals(Map.empty[String, Option[Any]], result)
  }

  @Test
  def multiLangValueToMap_booleanObject(): Unit = {
    val json = Json.obj("de" -> false)
    val result = JsonUtils.multiLangValueToMap(json)

    Assert.assertEquals(Map("de" -> Some(false)), result)
  }
}
