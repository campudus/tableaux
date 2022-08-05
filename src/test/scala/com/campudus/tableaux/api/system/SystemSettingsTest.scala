package com.campudus.tableaux.api.system

import com.campudus.tableaux.testtools.TableauxTestBase

import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.vertx.scala.core.json.Json

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(classOf[VertxUnitRunner])
class SystemSettingsTest extends TableauxTestBase {

  @Test
  def testLangtags(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)

    for {
      langtagsBeforeUpdate <- sendRequest("GET", "/system/settings/langtags")

      _ <- sqlConnection.query("UPDATE system_settings SET value=$$[\"en-GB\"]$$ WHERE key='langtags'")

      langtagsAfterUpdate <- sendRequest("GET", "/system/settings/langtags")

      _ <- sendRequest("POST", "/system/settings/langtags", Json.obj("value" -> Json.arr("de-DE")))

      langtagsAfterHttpUpdate <- sendRequest("GET", "/system/settings/langtags")
    } yield {
      assertEquals(Json.arr("de-DE", "en-GB"), langtagsBeforeUpdate.getJsonArray("value", Json.emptyArr()))
      assertEquals(Json.arr("en-GB"), langtagsAfterUpdate.getJsonArray("value", Json.emptyArr()))
      assertEquals(Json.arr("de-DE"), langtagsAfterHttpUpdate.getJsonArray("value", Json.emptyArr()))
    }
  }

  @Test
  def testSentryUrl(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(this.vertxAccess(), databaseConfig)

    for {
      sentryUrlBeforeInsert <- sendRequest("GET", "/system/settings/sentryUrl")

      _ <- sqlConnection.query("INSERT INTO system_settings (key, value) VALUES ('sentryUrl', 'https://example.com')")

      sentryUrlAfterInsert <- sendRequest("GET", "/system/settings/sentryUrl")

      _ <- sendRequest("POST", "/system/settings/sentryUrl", Json.obj("value" -> "https://example.de"))

      sentryUrlAfterUpdate <- sendRequest("GET", "/system/settings/sentryUrl")

      _ <- sqlConnection.query("DELETE FROM system_settings WHERE key = 'sentryUrl'")

      _ <- sendRequest("POST", "/system/settings/sentryUrl", Json.obj("value" -> "https://example.at"))

      sentryUrlAfterUpsert <- sendRequest("GET", "/system/settings/sentryUrl")
    } yield {
      assertEquals(None, Option(sentryUrlBeforeInsert.getValue("value")))

      assertEquals("https://example.com", sentryUrlAfterInsert.getString("value"))
      assertEquals("https://example.de", sentryUrlAfterUpdate.getString("value"))
      assertEquals("https://example.at", sentryUrlAfterUpsert.getString("value"))
    }
  }

  @Test
  def testRetrievingInvalidSetting(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {
    sendRequest("GET", "/system/settings/invalid")
  }
}
