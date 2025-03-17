package com.campudus.tableaux.verticles

import com.campudus.tableaux.verticles.EventClient._

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.eventbus.Message
import io.vertx.scala.ext.web.client.WebClient
import org.vertx.scala.core.json.{Json, JsonObject}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.LazyLogging

class CdnVerticle(cdnConfig: JsonObject) extends ScalaVerticle with LazyLogging {
  private lazy val eventBus = vertx.eventBus()
  private lazy val webClient: WebClient = WebClient.create(vertx)

  override def startFuture(): Future[_] = {
    eventBus.consumer(ADDRESS_FILE_CHANGED, purgeCdnFileUrl).completionFuture()
  }

  private def purgeCdnFileUrl(message: Message[JsonObject]): Unit = {
    val cdnUrl = cdnConfig.getString("url")
    val cdnApiKey = cdnConfig.getString("apiKey")
    val fileUuid = message.body().getString("uuid")
    val cdnPurgeUrl = s"$cdnUrl/purge"
    val cdnFileUrl = s"$cdnUrl/$fileUuid/*"

    logger.info(s"Purging CDN File URL: $cdnFileUrl")

    webClient
      .postAbs(cdnPurgeUrl)
      .addQueryParam("url", cdnFileUrl)
      .putHeader("AccessKey", cdnApiKey)
      .sendFuture().onComplete {
        case Success(_) =>
          message.reply("ok")
        case Failure(exception) =>
          val error = s"Failed purging CDN File URL: $cdnFileUrl, Reason: ${exception.getMessage()}"
          logger.error(error)
          message.fail(500, error)
      }
  }
}
