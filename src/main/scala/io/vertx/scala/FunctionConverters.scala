package io.vertx.scala

import io.vertx.core.{AsyncResult, Handler}
import io.vertx.scala.FutureHelper._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

trait FunctionConverters {

  type AsyncVoid = Handler[AsyncResult[Void]]
  type AsyncValue[A] = Handler[AsyncResult[A]]

  implicit def asyncResultToFuture[A, B](fn: (Handler[AsyncResult[A]]) => B): Future[A] = {
    // implicit tryToAsyncHandler
    futurify { p: Promise[A] =>
      fn({
        case Success(re) => p.success(re)
        case Failure(ex) => p.failure(ex)
      }: Try[A] => Unit)
    }
  }

  implicit def asyncVoidToFuture[A](fn: (Handler[AsyncResult[Void]]) => A): Future[Unit] = {
    // implicit tryToAsyncHandler
    futurify { p: Promise[Unit] =>
      fn({
        case Success(_) => p.success(())
        case Failure(ex) => p.failure(ex)
      }: Try[Void] => Unit)
    }
  }

  implicit def tryToAsyncHandler[A](tryHandler: Try[A] => Unit): Handler[AsyncResult[A]] = {
    // implicit functionToAnyHandler
    tryHandler.compose { ar: AsyncResult[A] =>
      if (ar.succeeded()) {
        Success(ar.result())
      } else {
        Failure(ar.cause())
      }
    }
  }

  implicit def functionToAnyHandler[A, B](function: (A) => B): Handler[A] = new Handler[A]() {
    override def handle(event: A): Unit = function(event)
  }

  implicit def functionToVoidHandler[B](function: => B): Handler[Void] = new Handler[Void]() {
    override def handle(event: Void): Unit = function
  }
}

object FunctionConverters extends FunctionConverters