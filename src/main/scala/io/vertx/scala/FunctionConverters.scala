package io.vertx.scala

import io.vertx.core.{AsyncResult, Handler}
import io.vertx.scala.FutureHelper._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

trait FunctionConverters {

  implicit def asyncResult[A, B](fn: (Handler[AsyncResult[A]]) => B): Future[A] = {
    futurify { p: Promise[A] =>
      fn({
        case Success(re) => p.success(re)
        case Failure(ex) => p.failure(ex)
      }: Try[A] => Unit)
    }
  }

  implicit def asyncVoid[A](fn: (Handler[AsyncResult[Void]]) => A): Future[Unit] = {
    futurify { p: Promise[Unit] =>
      fn({
        case Success(_) => p.success(())
        case Failure(ex) => p.failure(ex)
      }: Try[Void] => Unit)
    }
  }

  implicit def functionToHandler[A](handler: A => _): Handler[A] = new Handler[A]() {
    override def handle(event: A): Unit = handler(event)
  }

  implicit def tryToAsyncHandler[A](tryHandler: Try[A] => Unit): Handler[AsyncResult[A]] = {
    // implicit functionToHandler
    tryHandler.compose { ar: AsyncResult[A] =>
      if (ar.succeeded()) {
        Success(ar.result())
      } else {
        Failure(ar.cause())
      }
    }
  }

  implicit def lazyToVoidHandler(func: => Unit): Handler[Void] = new Handler[Void]() {
    override def handle(event: Void) = func
  }
}

object FunctionConverters extends FunctionConverters