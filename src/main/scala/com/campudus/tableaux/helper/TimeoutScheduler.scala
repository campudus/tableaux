package com.campudus.tableaux.helper

import java.util.concurrent.TimeoutException

import io.vertx.core.Vertx
import io.vertx.scala.FunctionConverters._

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}

object TimeoutScheduler {

  private def scheduleTimeout[T](vertx: Vertx, timeoutHandler: (Throwable) => Unit, after: Duration): Long = {
    vertx.setTimer(after.toMillis, {
      timeout: java.lang.Long =>
        timeoutHandler(new TimeoutException(s"Operation timed out after ${after.toMillis} millis"))
    })
  }

  implicit class TimeoutFuture[T](future: Future[T]) {

    import scala.concurrent.duration.DurationInt

    def withTimeout(timeoutMs: Int, name: String)(implicit vertx: Vertx, ec: ExecutionContext): Future[T] = withTimeout(DurationInt(timeoutMs).millis, name)

    def withTimeout(timeout: FiniteDuration, name: String)(implicit vertx: Vertx, ec: ExecutionContext): Future[T] = {
      val promise = Promise[T]()

      val timerId = TimeoutScheduler.scheduleTimeout(vertx, { e => promise.failure(e) }, timeout)
      future onComplete { case result => vertx.cancelTimer(timerId) }

      Future.firstCompletedOf(Seq(future, promise.future))
    }
  }

}