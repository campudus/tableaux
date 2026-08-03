package io.vertx.scala

import scala.concurrent.{Future, Promise}

object FutureHelper {

  def futurify[A](x: Promise[A] => ?): Future[A] = {
    val p = Promise[A]()
    x(p)
    p.future
  }
}
