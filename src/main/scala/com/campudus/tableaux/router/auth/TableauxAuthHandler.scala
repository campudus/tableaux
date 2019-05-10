package com.campudus.tableaux.router.auth

import com.campudus.tableaux.helper.VertxAccess
import com.campudus.tableaux.{AuthenticationException, RequestContext, TableauxConfig}
import io.vertx.core.Handler
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.User
import io.vertx.scala.ext.auth.jwt.JWTAuth
import io.vertx.scala.ext.web.RoutingContext
import io.vertx.scala.ext.web.handler.JWTAuthHandler

class TableauxAuthHandler(
    override val vertx: Vertx,
    authHandler: JWTAuthHandler,
    authProvider: JWTAuth,
    tableauxConfig: TableauxConfig
)(implicit requestContext: RequestContext)
    extends Handler[RoutingContext]
    with VertxAccess {

//  var jwkIsLoaded = false

//  val oauth2Provider: OAuth2Auth = KeycloakAuth.create(vertx, tableauxConfig.authConfig)
//  val oauth2Handler = OAuth2AuthHandler.create(authProvider)

  /**
    * Validates Access Token's "aud" and "iss" claims (https://tools.ietf.org/html/rfc7519).
    * If successful it stores the principal in requestContext for authorization.
    * If not context fails with statusCode 401.
    */
  override def handle(context: RoutingContext): Unit = {
    val user: Option[User] = context.user()

    val principal = user match {
      case Some(u) => u.principal()
      case _ => {
        val exception = AuthenticationException("No user in context")
        logger.error(exception.getMessage)
        context.fail(exception.statusCode, exception)
        throw exception
      }
    }

    checkAudience(context, principal)
    checkIssuer(context, principal)

    requestContext.principal = principal

    //    val aud = principal.getString("aud")
//
//    val principal = user match {
//      case Some(u) => u.principal()
//      case _ => {
//        val exception = AuthenticationException("No user in context")
//        logger.error(exception.getMessage)
//        context.fail(exception.statusCode, exception)
//        throw exception
//      }
//    }
//

//    val user: User = context.user().get
//    requestContext.principal = principal
//    val token: JsonObject = user.principal()

//    val accessTokenString: String = KeycloakHelper.rawAccessToken(context.user().get.principal)
//    val token: JsonObject = KeycloakHelper.parseToken(accessTokenString)
//

//    val jjj = new io.vertx.core.json.JsonObject()
//      .put("jwt", "asffasdff")
//      .put("options",
//        new io.vertx.core.json.JsonObject().put("audience", new io.vertx.core.json.JsonArray().add("paulo@server.com")))

//    authprovider.authenticateFuture(jjj).onComplete {
//      case Success(u: AccessToken) => {
//        println("decode success: " + u)
//
//        println("expired: " + u.expired())
//        println("tokenType: " + u.tokenType())
//
//        println("accessToken: " + u.accessToken().toString)
//        println("tokenType: " + u)
//
//      }
//      case Failure(cause) => {
//        println("failed: " + cause)
//        println("decode failed: " + cause.getMessage)
//      }
//    }

//    for {
//      at <- authProvider.decodeTokenFuture(accessTokenString)
//    } yield println("XXXXXX: " + at.tokenType())

    //    val tt: AccessToken = context.user().get.principal().asInstanceOf[AccessToken]
    //    println("XXX: " + tt.expired())

    //    oauth2
    //      .authenticateFuture(token)
    //      .onComplete {
    //        case Success(u) => {
    //          println("authentication success: " + u)
    //        }
    //        case Failure(cause) => {
    //          println("failed: " + cause)
    //          println("authentication failed: " + cause.getMessage)
    //        }
    //      }

//    println("XXX: " + token.getString("preferred_username"))
    println("XXX: Token Validation OK")
    context.next()
  }

  private def checkAudience(context: RoutingContext, principal: JsonObject) = {
    import scala.collection.JavaConverters._

    val audiences: Seq[String] = principal.getValue("aud") match {
      case s: String => List(s)
      case o: JsonArray => o.asScala.map({ case item: String => item }).toList
    }

    if (!audiences.contains(getAudience)) {
      val exception = AuthenticationException(s"Audience '$getAudience' must be one of $audiences")
      logger.error(exception.getMessage)
      context.fail(exception.statusCode, exception)
      throw exception
    }
  }

  private def checkIssuer(context: RoutingContext, principal: JsonObject) = {
    val issuer: String = principal.getString("iss", "_invalid_")

    if (issuer != getIssuer) {
      val exception = AuthenticationException(s"Issuer '$issuer' doesn't match to '$getIssuer'")
      logger.error(exception.getMessage)
      context.fail(exception.statusCode, exception)
      throw exception
    }
  }

  //  private def loadJwkIfNecessary: Future[Unit] = {
  //    if (jwkIsLoaded) {
  //      return Future.successful(())
  //    }
  //
  //    val jwkFuture = authProvider.loadJWKFuture()
  //
  //    jwkFuture.onComplete({
  //      case Success(_) =>
  //        jwkIsLoaded = true
  //        logger.info(s"JWK loading successfully")
  //      case Failure(e) =>
  //        logger.error("JWK loading failed", e)
  //    })
  //
  //    jwkFuture
  //  }
  def getAudience = tableauxConfig.authConfig.getString("audience")
  def getIssuer = tableauxConfig.authConfig.getString("issuer")
}

//object TableauxAuthHandler {
//
//  def apply(vertx: Vertx, authProvider: JWTAuth): TableauxAuthHandler = {
//    val tah = new TableauxAuthHandler(vertx, authProvider)
////    tah.loadJwkIfNecessary
//    tah
//  }
//}
