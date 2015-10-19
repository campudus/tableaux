package com.campudus.tableaux.helper

import io.vertx.scala.FutureHelper._

import scala.concurrent.{Future, Promise}

@Deprecated object FutureUtils {
  @Deprecated def promisify[X](stuff: Promise[X] => _): Future[X] = futurify(stuff)
}
