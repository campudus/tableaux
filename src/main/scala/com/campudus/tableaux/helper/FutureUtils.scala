package com.campudus.tableaux.helper

import scala.concurrent.{Future, Promise}

/**
 * Created by alexandervetter on 07.05.15.
 */
object FutureUtils {
  def promisify[X](stuff: Promise[X] => _): Future[X] = {
    val p = Promise[X]()
    stuff(p)
    p.future
  }
}
