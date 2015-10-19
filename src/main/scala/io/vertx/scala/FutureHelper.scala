package io.vertx.scala

import io.vertx.core.{Handler, AsyncResult}

import scala.concurrent.{Future, Promise}
import scala.util.{Try, Failure, Success}

import FunctionConverters._

object FutureHelper {

  def futurify[A](x: Promise[A] => _): Future[A] = {
    val p = Promise[A]()
    x(p)
    p.future
  }

  def asyncResultToFuture[T](fn: Handler[AsyncResult[T]] => _): Future[T] = futurify { p: Promise[T] =>
    val t = {
      case Success(result) => p.success(result)
      case Failure(ex) => p.failure(ex)
    }: Try[T] => Unit
    fn(t)
  }
}
