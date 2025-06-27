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
      setting <- sendRequest("PUT", "/user/settings/global/filterReset", Json.obj("value" -> true))
    } yield {
      assertEquals("filterReset", setting.getString("key"))
      assertEquals("global", setting.getString("kind"))
      assertEquals(true, setting.getBoolean("value"))
    }
  }

  @Test
  def testCreateGlobalSettingNoJson(implicit c: TestContext): Unit = exceptionTest("error.json.notfound") {
    for {
      setting <- sendRequest("PUT", "/user/settings/global/filterReset")
    } yield ()
  }

  @Test
  def testCreateGlobalSettingNoValue(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    for {
      setting <- sendRequest("PUT", "/user/settings/global/filterReset", Json.obj())
    } yield ()
  }

  @Test
  def testCreateGlobalSettingInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest("PUT", "/user/settings/global/filterReset", Json.obj("value" -> "some text"))
  }

  @Test
  def testCreateSettingInvalidKey(implicit c: TestContext): Unit = exceptionTest("error.request.usersetting.invalid") {
    sendRequest("PUT", "/user/settings/global/doesNotExist", Json.obj("value" -> true))
  }

  @Test
  def testCreateTableSetting(implicit c: TestContext): Unit = okTest {
    for {
      setting <- sendRequest(
        "PUT",
        "/user/settings/table/rowsFilter/1",
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
        "/user/settings/table/rowsFilter/1",
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
  def testCreateTableSettingNoTableId(implicit c: TestContext): Unit = exceptionTest("NOT FOUND") {
    sendRequest(
      "PUT",
      "/user/settings/table/rowsFilter",
      Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "ID", "contains", "Sunset")))
    )
  }

  @Test
  def testCreateTableSettingInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest(
      "PUT",
      "/user/settings/table/rowsFilter/1",
      Json.obj("value" -> true)
    )
  }

  @Test
  def testCreateFilterSetting(implicit c: TestContext): Unit = okTest {
    for {
      setting <- sendRequest(
        "PUT",
        s"/user/settings/filter/presetFilter",
        Json.obj(
          "name" -> "Simple Filter 1",
          "value" -> Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black"))
        )
      )
    } yield {
      assertEquals("presetFilter", setting.getString("key"))
      assertEquals("filter", setting.getString("kind"))
      assertEquals("Simple Filter 1", setting.getString("name"))
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
        s"/user/settings/filter/presetFilter",
        Json.obj(
          "name" -> "Complex Filter 1",
          "value" -> settingValue
        )
      )
    } yield {
      assertEquals("presetFilter", setting.getString("key"))
      assertEquals("filter", setting.getString("kind"))
      assertEquals("Complex Filter 1", setting.getString("name"))
      assertJSONEquals(settingValue, setting.getJsonObject("value"))
    }
  }

  @Test
  def testCreateFilterSettingNoName(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest(
      "PUT",
      "/user/settings/filter/presetFilter",
      Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black")))
    )
  }

  @Test
  def testCreateFilterSettingInvalidJson(implicit c: TestContext): Unit = exceptionTest("error.json.invalid") {
    sendRequest(
      "PUT",
      "/user/settings/filter/presetFilter",
      Json.obj(
        "name" -> "Filter Invalid",
        "value" -> true
      )
    )
  }

  @Test
  def testRetrieveSettings(implicit c: TestContext): Unit = okTest {
    for {
      // create some settings
      _ <- sendRequest("PUT", "/user/settings/global/filterReset", Json.obj("value" -> true))
      _ <- sendRequest("PUT", "/user/settings/global/sortingReset", Json.obj("value" -> false))
      _ <- sendRequest("PUT", "/user/settings/global/sortingDesc", Json.obj("value" -> false))
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/visibleColumns/1",
        Json.obj("value" -> Json.arr(1, 2, 3))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/rowsFilter/2",
        Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "ID", "contains", "Dusk")))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/filter/presetFilter",
        Json.obj(
          "name" -> "Simple Filter",
          "value" -> Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black"))
        )
      )
      // retrieve settings
      allSettings <- sendRequest("GET", "/user/settings")
      globalSettings <- sendRequest("GET", "/user/settings/global")
      tableSettings <- sendRequest("GET", "/user/settings/table")
      filterSettings <- sendRequest("GET", "/user/settings/filter")
    } yield {
      assertEquals(6, allSettings.getJsonArray("settings").size())
      assertEquals(3, globalSettings.getJsonArray("settings").size())
      assertEquals(2, tableSettings.getJsonArray("settings").size())
      assertEquals(1, filterSettings.getJsonArray("settings").size())
    }
  }

  @Test
  def testRetrieveTableSettingsById(implicit c: TestContext): Unit = okTest {
    for {
      // create some table settings for different tables
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/columnOrdering/1",
        Json.obj("value" -> Json.arr(
          Json.obj("id" -> 1, "idx" -> 1),
          Json.obj("id" -> 2, "idx" -> 0)
        ))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/visibleColumns/1",
        Json.obj("value" -> Json.arr(1, 2, 3))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/rowsFilter/1",
        Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "ID", "contains", "Dusk")))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/annotationHighlight/2",
        Json.obj("value" -> "important")
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/columnWidths/2",
        Json.obj("value" ->  Json.obj("1" -> 250, "2" -> 300))
      )
      // retrieve settings
      tableSettings <- sendRequest("GET", "/user/settings/table")
      tableSettingsId1 <- sendRequest("GET", "/user/settings/table/1")
      tableSettingsId2 <- sendRequest("GET", "/user/settings/table/2")
    } yield {
      assertEquals(5, tableSettings.getJsonArray("settings").size())
      assertEquals(3, tableSettingsId1.getJsonArray("settings").size())
      assertEquals(2, tableSettingsId2.getJsonArray("settings").size())
    }
  }

  @Test
  def testDeleteTableSetting(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/visibleColumns/1",
        Json.obj("value" -> Json.arr(1, 2, 3))
      )
      tableSettingsAfterCreate <- sendRequest("GET", "/user/settings/table")
      _ <- sendRequest("DELETE", "/user/settings/table/visibleColumns/1")
      tableSettingsAfterDelete <- sendRequest("GET", "/user/settings/table")
    } yield {
      assertEquals(1, tableSettingsAfterCreate.getJsonArray("settings").size())
      assertEquals(0, tableSettingsAfterDelete.getJsonArray("settings").size())
    }
  }

  @Test
  def testDeleteTableSettings(implicit c: TestContext): Unit = okTest {
    for {
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/visibleColumns/1",
        Json.obj("value" -> Json.arr(1, 2, 3))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/columnOrdering/1",
        Json.obj("value" -> Json.arr(
          Json.obj("id" -> 1, "idx" -> 1),
          Json.obj("id" -> 2, "idx" -> 0)
        ))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/rowsFilter/1",
        Json.obj("value" -> Json.obj("filters" -> Json.arr("value", "ID", "contains", "Dusk")))
      )
      _ <- sendRequest(
        "PUT",
        "/user/settings/table/annotationHighlight/1",
        Json.obj("value" -> "important")
      )
      tableSettingsAfterCreate <- sendRequest("GET", "/user/settings/table")
      _ <- sendRequest("DELETE", "/user/settings/table/1")
      tableSettingsAfterDelete <- sendRequest("GET", "/user/settings/table")
    } yield {
      assertEquals(4, tableSettingsAfterCreate.getJsonArray("settings").size())
      assertEquals(0, tableSettingsAfterDelete.getJsonArray("settings").size())
    }
  }

  @Test
  def testDeleteFilterSetting(implicit c: TestContext): Unit = okTest {
    for {
      settingId <- sendRequest(
        "PUT",
        "/user/settings/filter/presetFilter",
        Json.obj(
          "name" -> "Simple Filter 1",
          "value" -> Json.obj("filters" -> Json.arr("value", "identifier", "starts-with", "black"))
        )
      ).map(_.getLong(("id")))
      filterSettingsAfterCreate <- sendRequest("GET", "/user/settings/filter")
      _ <- sendRequest("DELETE", s"/user/settings/filter/presetFilter/$settingId")
      filterSettingsAfterDelete <- sendRequest("GET", "/user/settings/filter")
    } yield {
      assertEquals(1, filterSettingsAfterCreate.getJsonArray("settings").size())
      assertEquals(0, filterSettingsAfterDelete.getJsonArray("settings").size())
    }
  }
}
