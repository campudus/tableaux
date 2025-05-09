package com.campudus.tableaux.helper

import com.campudus.tableaux.database.model.CreateHistoryModel
import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.{Assert, Test}
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class OmitNonChanges extends TableauxTestBase {

  @Test
  def omitEqualValues(): Unit = {
    val oldMap = Map("de" -> Some("HallWrong"), "en" -> Some("Hello"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val result = JsonUtils.omitNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("de" -> Some("Hallo")), result)
  }

  @Test
  def ignoreAdditionalValuesInOldMap(): Unit = {
    val oldMap = Map("de" -> Some("HallWrong"), "en" -> Some("Hello"), "fr" -> Some("Bonjour"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val result = JsonUtils.omitNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("de" -> Some("Hallo")), result)
  }

  @Test
  def doNotOmitNullValuesInNewMap(): Unit = {
    val oldMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> None)
    val result = JsonUtils.omitNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("en" -> None), result)
  }

  @Test
  def returnEmptyMapIfAllKeysAreOmitted(): Unit = {
    val oldMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val newMap = Map("de" -> Some("Hallo"), "en" -> Some("Hello"))
    val result = JsonUtils.omitNonChanges(newMap, oldMap)

    Assert.assertEquals(Map.empty[String, Option[Any]], result)
  }

  @Test
  def doOmitNullValuesIfNotExistsInOldMap(): Unit = {
    val oldMap = Map("DE" -> Some("200"), "GB" -> Some("300"))
    val newMap = Map("DE" -> Some("400"), "FR" -> Option(null))
    val result = JsonUtils.omitNonChanges(newMap, oldMap)

    Assert.assertEquals(Map("DE" -> Some("400")), result)
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
