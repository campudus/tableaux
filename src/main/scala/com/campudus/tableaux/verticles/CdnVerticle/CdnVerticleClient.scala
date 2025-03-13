package com.campudus.tableaux.verticles.CdnVerticle

import com.campudus.tableaux.database.domain.ExtendedFile
import com.campudus.tableaux.helper.VertxAccess

import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.{EventBus, Message}
import org.vertx.scala.core.json.Json

import scala.concurrent.Future

object CdnVerticleClient {

  def apply(vertx: Vertx): CdnVerticleClient = {
    new CdnVerticleClient(vertx)
  }
}

class CdnVerticleClient(val vertx: Vertx) extends VertxAccess {
  import CdnVerticle._

  val eventBus: EventBus = vertx.eventBus()

  def fileChanged(oldFile: ExtendedFile): Future[Unit] = {
    val message = oldFile.getJson
    eventBus.sendFuture[String](ADDRESS_FILE_CHANGED, message).map(_ => {}).recover { case _: Throwable => }
  }
}
