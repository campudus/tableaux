package com.campudus.tableaux.verticles

import com.campudus.tableaux.helper.Json
import com.campudus.tableaux.verticles.EventClient._

import io.vertx.core.eventbus.Message
import io.vertx.ext.web.client.WebClient
import io.vertx.lang.scala.{ScalaVerticle, *}
import io.vertx.lang.scala.json.JsonObject

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging

class CdnVerticle(cdnConfig: JsonObject, customWebClient: Option[WebClient] = None) extends ScalaVerticle
    with LazyLogging {
  private lazy val eventBus = vertx.eventBus()

  private lazy val webClient: WebClient = customWebClient match {
    case Some(webClient) => webClient
    case None => WebClient.create(vertx)
  }

  override def asyncStart: Future[Unit] = {
    val promise = Promise[Unit]()
    eventBus
      .consumer(ADDRESS_FILE_CHANGED, purgeCdnFileUrl)
      .completionHandler(ar => if (ar.succeeded()) promise.success(()) else promise.failure(ar.cause()))
    promise.future
  }

  private def purgeCdnFileUrl(message: Message[JsonObject]): Unit = {
    val cdnUrl = cdnConfig.getString("url")
    val cdnApiKey = cdnConfig.getString("apiKey")
    val fileUuid = message.body().getString("uuid")
    val cdnPurgeUrl = s"$cdnUrl/purge"
    val cdnFileUrl = s"$cdnUrl/$fileUuid/*"

    logger.info(s"Purging CDN File URL: $cdnFileUrl")

    val request = webClient.postAbs(cdnPurgeUrl)

    request.addQueryParam("url", cdnFileUrl)
    request.putHeader("AccessKey", cdnApiKey)

    request.send().asScala.onComplete {
      case Success(_) =>
        message.reply("ok")
      case Failure(exception) =>
        val error = s"Failed purging CDN File URL: $cdnFileUrl, Reason: ${exception.getMessage()}"
        logger.error(error)
        message.fail(500, error)
    }
  }
}
