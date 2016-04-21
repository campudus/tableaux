package com.campudus.tableaux.api.system

import com.campudus.tableaux.testtools.TableauxTestBase
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.scala.SQLConnection
import org.junit.Test
import org.junit.runner.RunWith
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class SystemSettingsTest extends TableauxTestBase {

  @Test
  def testLangtags(implicit c: TestContext): Unit = okTest {
    val sqlConnection = SQLConnection(verticle, databaseConfig)

    for {
      langtags <- sendRequest("GET", "/system/settings/langtags")

      _ <- sqlConnection.query("UPDATE system_settings SET value=$$[\"en-GB\"]$$ WHERE key='langtags'")

      langtagsAfterUpdate <- sendRequest("GET", "/system/settings/langtags")
    } yield {
      assertEquals(Json.arr("de-DE", "en-GB"), langtags.getJsonArray("value", Json.emptyArr()))
      assertEquals(Json.arr("en-GB"), langtagsAfterUpdate.getJsonArray("value", Json.emptyArr()))
    }
  }

  @Test
  def testRetrievingInvalidSetting(implicit c: TestContext): Unit = exceptionTest("error.request.invalid") {
    sendRequest("GET", "/system/settings/invalid")
  }
}
