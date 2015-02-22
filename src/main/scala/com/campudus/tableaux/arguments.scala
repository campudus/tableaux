package com.campudus.tableaux

sealed trait ArgumentCheck

case object OkArg extends ArgumentCheck

case class FailArg(message: String) extends ArgumentCheck

/**
 * @author <a href="http://www.campudus.com">Joern Bernhardt</a>.
 */
object ArgumentChecker {

  def notNull(x: Any): ArgumentCheck = if (x != null) OkArg else FailArg("Argument is null")

  def greaterZero(x: Long): ArgumentCheck = if (x > 0) OkArg else FailArg(s"Argument $x is not greater than zero")

  def checkSeq(x: Seq[_]): Seq[ArgumentCheck] = x flatMap { matcher(_) }

  private def matcher(x: Any): Seq[ArgumentCheck] = x match {
    case i: Seq[_] => checkSeq(i)
    case (a, b) => matcher(a) ++ matcher(b)
    case i: Long => Seq(greaterZero(i))
    case i => Seq(notNull(i))
  }

  def checkArguments(args: ArgumentCheck*): Unit = {
    val failedArgs: Vector[String] = args.zipWithIndex.foldLeft(Vector[String]()) {
      case (v, (FailArg(ex), idx)) => v :+ s"($idx) $ex"
      case (v, (OkArg, idx)) => v
    }

    if (failedArgs.nonEmpty) throw new IllegalArgumentException(failedArgs.mkString("\n"))
  }

}
