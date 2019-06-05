package com.campudus.tableaux.helper

import com.campudus.tableaux.testtools.{TableauxTestBase, TokenHelper}
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.runner.RunWith
import org.junit.{Assert, Test}
import org.vertx.scala.core.json.Json

@RunWith(classOf[VertxUnitRunner])
class TokenHelperTest extends TableauxTestBase {

  @Test
  def create(): Unit = {
    val th = TokenHelper(this.vertxAccess())

    val claims = Json.obj("foo" -> "bar", "ans" -> 42)

    val encodedToken = "" +
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJmb28iOiJiYXIiLCJhbnMiOjQyfQ.DZgAsGBv1Wj3ExfV2wr4gP0YDxj" +
      "lnFkqNCrczoezUjqRpvFuOX7IQso56E61KeCXncZQJ4x_R-8uWHHFoD54rIyVBiVN4R3GtfuCf0QaIKrVrQTs2cMCSDOU4D" +
      "oB0XrkvRvhxOqI1Bir9or7TByQluzGiACqDjpBiGlgtOsKLLgd74eRe7p1V6C9TbleBgxGWumRQuWNVzmyASUM0TCtnyz_V" +
      "VJ3Pw5DIkZu5Pd-E1S2uYGvLiyC2JGHwnI4Lss2kfcX_X3mPRZgKBCUTcj_1mF6Lj8TuylGSIBDZvtex3WQcySiV7FyVEGS" +
      "aPB4Ian974rWLdrnHf3X2t81W6Gyog"
    Assert.assertEquals(encodedToken, th.generateToken(claims))
  }
}
