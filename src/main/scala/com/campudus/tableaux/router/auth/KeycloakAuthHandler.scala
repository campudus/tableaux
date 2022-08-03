package com.campudus.tableaux.router.auth

import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.{AuthenticationException, TableauxConfig}
import io.vertx.core.Handler
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.User
import io.vertx.scala.ext.auth.oauth2.KeycloakHelper
import io.vertx.scala.ext.web.RoutingContext

import scala.collection.JavaConverters._
import org.vertx.scala.core.json.Json
import com.campudus.tableaux.helper.JsonUtils
import scala.util.Try

object KeycloakAuthHandler {
  val USER_ROLES: String = "userRoles"
  val USER_NAME: String = "userName"

  // var principal: JsonObject = Json.emptyObj() // scalastyle:ignore

  // // for testing purposes, unit tests must reset the principal in setUp method
  // def resetPrincipal(): Unit = principal = Json.emptyObj()

  // def getPrincipleString(name: String): String = {
  //   principal.getString(name)
  // }

  // // field has to be mutable and is only set from RouterRegistry::routes
  // var cookies: Set[Cookie] = Set.empty[Cookie] // scalastyle:ignore

  // def getCookieValue(name: String, defaultValue: String): String = {
  //   cookies.find(_.getName() == name).map(_.getValue()).getOrElse(defaultValue)
  // }

  def apply(vertx: Vertx, config: TableauxConfig): KeycloakAuthHandler = {
    new KeycloakAuthHandler(vertx, config)
  }

  // gets realm roles from RoutingContext
  def getRealmRoles(rc: RoutingContext): Seq[String] = {
    rc.get(USER_ROLES).asInstanceOf[Seq[String]]
  }

  private def extractRealmRoles(tokenPayload: JsonObject): Seq[String] = {
    val roles: JsonArray =
      tokenPayload
        .getJsonObject("realm_access", Json.emptyObj())
        .getJsonArray("roles", Json.emptyArr())

    JsonUtils.asSeqOf[String](roles)
  }

  def getCookieValue(defaultValue: String, name: String, rc: RoutingContext): String =
    rc.cookies().toSet
      .find(_.getName() == name)
      .map(_.getValue())
      .getOrElse(defaultValue)

  private def extractUserName(tokenPayload: JsonObject, rc: RoutingContext): String =
    Option(tokenPayload
      .getString("preferred_username"))
      .getOrElse(getCookieValue("unknown", "userName", rc))

  def getUserName(rc: RoutingContext): String =
    rc.get(USER_NAME).asInstanceOf[String]
}

class KeycloakAuthHandler(override val vertx: Vertx, tableauxConfig: TableauxConfig)
    extends Handler[RoutingContext] with VertxAccess {

  // private def extractRealmRoles(tokenPayload: JsonObject): Seq[String] = {
  //   val roles: JsonArray =
  //     tokenPayload
  //       .getJsonObject("realm_access", Json.emptyObj())
  //       .getJsonArray("roles", Json.emptyArr())

  //   JsonUtils.asSeqOf[String](roles)
  // }

  // private def getCookieValue(defaultValue: String, name: String, rc: RoutingContext): String =
  //   rc.cookies().toSet
  //     .find(_.getName() == name)
  //     .map(_.getValue())
  //     .getOrElse(defaultValue)

  // private def extractUserName(tokenPayload: JsonObject, rc: RoutingContext): Seq[String] =
  //   Option(tokenPayload
  //     .getString("preferred_username"))
  //     .getOrElse(getCookieValue("unknown", "userName", context))

  /**
    * Validates Access Token's "aud" and "iss" claims (https://tools.ietf.org/html/rfc7519). If successful it stores the
    * principal in requestContext for authorization. If not context fails with statusCode 401.
    */
  override def handle(rc: RoutingContext): Unit = {
    val user: Option[User] = rc.user()

    // logger.info(s"User11 ${contextUser.get.principal()}")

    val tokenPayload = user match {
      case Some(u) => KeycloakHelper.accessToken(u.principal())
      case _ =>
        val exception = AuthenticationException("No user in context")
        logger.error(exception.getMessage)
        rc.fail(exception.statusCode, exception)
        throw exception
    }

    checkAudience(rc, tokenPayload)
    checkIssuer(rc, tokenPayload)

    // TODO remove requestContext
    // requestContext.principal = tokenPayload

    rc.put(KeycloakAuthHandler.USER_ROLES, KeycloakAuthHandler.extractRealmRoles(tokenPayload))
    rc.put(KeycloakAuthHandler.USER_NAME, KeycloakAuthHandler.extractUserName(tokenPayload, rc))

    rc.next()
  }

  private def checkAudience(context: RoutingContext, tokenPayload: JsonObject): Unit = {
    if (shouldVerifyAudience) {
      val audiences: Seq[String] = tokenPayload.getValue("aud") match {
        case s: String => Seq(s)
        case o: JsonArray => o.asScala.map({ case item: String => item }).toSeq
      }

      if (!audiences.contains(getAudience)) {
        val exception = AuthenticationException(
          s"Audiences $audiences of the request doesn't contain configured service audience (resource) '$getAudience'"
        )
        logger.error(exception.getMessage)
        context.fail(exception.statusCode, exception)
        throw exception
      }
    }
  }

  private def checkIssuer(context: RoutingContext, tokenPayload: JsonObject): Unit = {
    val issuer: String = tokenPayload.getString("iss", "_invalid_")

    if (issuer != getIssuer) {
      val exception = AuthenticationException(
        s"Issuer '$issuer' of the request doesn't match to configured service issuer '$getIssuer'"
      )
      logger.error(exception.getMessage)
      context.fail(exception.statusCode, exception)
      throw exception
    }
  }

  def getAudience: String = tableauxConfig.authConfig.getString("resource")
  def shouldVerifyAudience: Boolean = tableauxConfig.authConfig.getBoolean("verify-token-audience", false)
  def getIssuer: String = tableauxConfig.authConfig.getString("issuer")
}
