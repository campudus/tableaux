package com.campudus.tableaux.api.user

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class UserSettingsTest extends TableauxTestBase {

  @Test
  def testCreateGlobalSetting(implicit c: TestContext): Unit = okTest {
    for {
      setting <- sendRequest("PUT", "/user/settings/filterReset", Json.obj("value" -> true))
    } yield {
      assertEquals("filterReset", setting.getString("key"))
      assertEquals("global", setting.getString("kind"))
      assertEquals(true, setting.getBoolean("value"))
    }
  }

  @Test
  def testCreateGlobalSettingInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest("PUT", "/user/settings/filterReset", Json.obj("value" -> "some text"))
  }

  @Test
  def testCreateSettingInvalidKey(implicit c: TestContext): Unit = exceptionTest("error.request.usersetting.invalid") {
    sendRequest("PUT", "/user/settings/doesNotExist", Json.obj("value" -> true))
  }

  @Test
  def testCreateTableSetting(implicit c: TestContext): Unit = okTest {
    for {
      setting <- sendRequest(
        "PUT",
        "/user/settings/rowsFilter?tableId=1",
        Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "ID", "contains", "Sunset")))
      )
    } yield {
      assertEquals("rowsFilter", setting.getString("key"))
      assertEquals("table", setting.getString("kind"))
      assertEquals(1L, setting.getLong("tableId"))
      assertJSONEquals(
        Json.obj("filters" -> Json.arr("value", "ID", "contains", "Sunset")),
        setting.getJsonObject("value")
      )
    }
  }

  @Test
  def testCreateTableSettingComplexValue(implicit c: TestContext): Unit = okTest {
    val settingValue = Json.obj(
      "sortColumnName" -> "ID",
      "sortDirection" -> "asc",
      "filters" -> Json.arr(
        "and",
        Json.arr("value", "ID", "contains", "Sunset"),
        Json.arr(
          "or",
          Json.arr("value", "ID", "contains", "Light"),
          Json.arr("value", "ID", "contains", "Dark")
        ),
        Json.arr("annotation", "flag-type", "important", "is-set")
      )
    )

    for {
      setting <- sendRequest(
        "PUT",
        "/user/settings/rowsFilter?tableId=1",
        Json.obj("value" -> settingValue)
      )
    } yield {
      assertEquals("rowsFilter", setting.getString("key"))
      assertEquals("table", setting.getString("kind"))
      assertEquals(1L, setting.getLong("tableId"))
      assertJSONEquals(settingValue, setting.getJsonObject("value"))
    }
  }

  @Test
  def testCreateTableSettingNoTableId(implicit c: TestContext): Unit = exceptionTest("error.arguments") {
    sendRequest(
      "PUT",
      "/user/settings/rowsFilter",
      Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "ID", "contains", "Sunset")))
    )
  }

  @Test
  def testCreateTableSettingInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest(
      "PUT",
      "/user/settings/rowsFilter?tableId=1",
      Json.obj("value" -> Json.obj("filters" -> Json.obj("value" -> true)))
    )
  }

  @Test
  def testCreateFilterSetting(implicit c: TestContext): Unit = okTest {
    for {
      setting <- sendRequest(
        "PUT",
        s"/user/settings/presetFilter?name=simple",
        Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black")))
      )
    } yield {
      assertEquals("presetFilter", setting.getString("key"))
      assertEquals("filter", setting.getString("kind"))
      assertEquals("simple", setting.getString("name"))
      assertJSONEquals(
        Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black")),
        setting.getJsonObject("value")
      )
    }
  }

  @Test
  def testCreateFilterSettingComplexValue(implicit c: TestContext): Unit = okTest {
    val filterName = "complex"
    val settingValue = Json.obj(
      "sortColumnName" -> "mainColor",
      "sortDirection" -> "desc",
      "filters" -> Json.arr(
        "and",
        Json.arr(
          "or",
          Json.arr("value", "identifier", "starts-with", "black"),
          Json.arr("value", "identifier", "starts-with", "white")
        ),
        Json.arr("value", "mainColor", "equals", "green"),
        Json.arr("annotation", "type", "info", "is-set"),
        Json.arr("row-prop", "final", "is-set")
      )
    )

    for {
      setting <- sendRequest(
        "PUT",
        s"/user/settings/presetFilter?name=complex",
        Json.obj("value" -> settingValue)
      )
    } yield {
      assertEquals("presetFilter", setting.getString("key"))
      assertEquals("filter", setting.getString("kind"))
      assertEquals("complex", setting.getString("name"))
      assertJSONEquals(settingValue, setting.getJsonObject("value"))
    }
  }

  @Test
  def testCreateFilterSettingNoName(implicit c: TestContext): Unit = exceptionTest("error.arguments") {
    sendRequest(
      "PUT",
      "/user/settings/presetFilter",
      Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black")))
    )
  }

  @Test
  def testCreateFilterSettingInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest(
      "PUT",
      "/user/settings/presetFilter?name=invalid",
      Json.obj("value" -> Json.obj(
        "filters" -> Json.arr("value", "identifier", "starts-with", "black"),
        "sortDirection" -> "none" // invalid direction
      ))
    )
  }
}
