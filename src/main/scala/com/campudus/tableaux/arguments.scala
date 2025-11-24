package com.campudus.tableaux

import com.campudus.tableaux.database.domain.UnionTable

import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.util.{Failure, Success, Try}

sealed trait ArgumentCheck[A] extends Product with Serializable {

  def map[B](f: A => B): ArgumentCheck[B]

  def flatMap[B](f: A => ArgumentCheck[B]): ArgumentCheck[B]

  def get: A

  def toOption: Option[A]
}

case class OkArg[A](value: A) extends ArgumentCheck[A] {

  override def map[B](f: A => B): ArgumentCheck[B] = OkArg(f(value))

  override def flatMap[B](f: A => ArgumentCheck[B]): ArgumentCheck[B] = f(value)

  override def get: A = value

  override def toOption: Option[A] = Some(get)
}

case class FailArg[A](ex: CustomException) extends ArgumentCheck[A] {

  override def map[B](f: A => B): ArgumentCheck[B] = FailArg(ex)

  override def flatMap[B](f: A => ArgumentCheck[B]): ArgumentCheck[B] = FailArg(ex)

  override def get: A = throw ex

  override def toOption: Option[A] = None
}

/**
  * @author
  *   <a href="http://www.campudus.com">Joern Bernhardt</a>.
  */
object ArgumentChecker {

  def notNull[A](x: => A, name: String): ArgumentCheck[A] = {
    val tryNullCheck = Try[ArgumentCheck[A]]({
      Option(x) match {
        case Some(y) =>
          OkArg(y.asInstanceOf[A])

        case None =>
          FailArg(InvalidJsonException(s"Warning: $name is null", "null"))
      }
    })

    tryNullCheck match {
      case Success(check) =>
        check
      case Failure(npe: NullPointerException) =>
        FailArg(InvalidJsonException(s"Warning: $name is null", "null"))
      case Failure(cce: ClassCastException) =>
        FailArg(InvalidJsonException(s"Warning: $name should be another type. Error: ${cce.getMessage}", "invalid"))
      case Failure(ex) =>
        FailArg(UnprocessableEntityException(ex.getMessage, ex))
    }
  }

  def greaterThan(x: Long, than: Long, name: String): ArgumentCheck[Long] = greaterThan(x, than, Option(name))

  def greaterThan(x: Long, than: Long, name: Option[String] = None): ArgumentCheck[Long] = {
    if (x > than) {
      OkArg(x)
    } else {
      FailArg({
        val argument = name.map(n => s"($n) $x").getOrElse(s"$x")
        InvalidJsonException(s"Argument $argument is not greater than $than.", "invalid")
      })
    }
  }

  def greaterZero(x: Long): ArgumentCheck[Long] = {
    greaterThan(x, 0)
  }

  def nonEmpty[A](seq: Seq[A], name: String): ArgumentCheck[Seq[A]] = {
    if (seq.nonEmpty) {
      OkArg(seq)
    } else {
      FailArg(InvalidJsonException(s"Warning: $name is empty.", "empty"))
    }
  }

  def isDefined[A](option: Option[A], name: String): ArgumentCheck[A] = {
    if (option.isDefined) {
      OkArg(option.get)
    } else {
      FailArg(ParamNotFoundException(s"query parameter $name not found"))
    }
  }

  def isDefined(options: Seq[Option[_]], name: String = ""): ArgumentCheck[Unit] = {
    val empty = !options.exists(_.isDefined)

    if (empty) {
      FailArg(InvalidRequestException(s"None of these options has a (valid) value. ($name)"))
    } else {
      OkArg(())
    }
  }

  def oneOf[A](x: => A, list: List[A], name: String): ArgumentCheck[A] = {
    if (list.contains(x)) {
      OkArg(x)
    } else {
      FailArg(InvalidRequestException(s"'$name' value needs to be one of ${list.mkString("'", "', '", "'")}."))
    }
  }

  def hasArray(field: String, json: JsonObject): ArgumentCheck[JsonArray] = notNull(json.getJsonArray(field), field)

  def hasLong(field: String, json: JsonObject): ArgumentCheck[Long] = notNull(json.getLong(field).longValue(), field)

  def hasString(field: String, json: JsonObject): ArgumentCheck[String] = notNull(json.getString(field), field)

  def tryCast[A](elem: Any): ArgumentCheck[A] = {
    tryMap(
      (x: Any) => x.asInstanceOf[A],
      InvalidJsonException(s"Warning: $elem should not be ${elem.getClass}", "invalid")
    )(elem)
  }

  def checkSameLengthsAndZip[A, B](list1: Seq[A], list2: Seq[B]): ArgumentCheck[Seq[(A, B)]] = {
    if (list1.size == list2.size) {
      OkArg(list1.zip(list2))
    } else {
      FailArg(NotEnoughArgumentsException("lists are not equally sized"))
    }
  }

  def checkForAllValues[A](json: JsonObject, predicate: (A => Boolean), name: String): ArgumentCheck[JsonObject] = {
    import scala.collection.JavaConverters._
    val fields = json.fieldNames().asScala.toList
    val failedFields = fields.filter(field => {
      Try(predicate(json.getValue(field).asInstanceOf[A])) match {
        case Failure(_) | Success(false) =>
          true
        case _ =>
          false
      }
    })

    failedFields match {
      case Nil =>
        OkArg(json)
      case field :: _ if Option(json.getValue(field)).isEmpty =>
        FailArg(InvalidJsonException(s"Warning: $name has value '$field' pointing at null.", "invalid"))
      case field :: _ =>
        FailArg(InvalidJsonException(s"Warning: $name has incorrectly typed value at key '$field'.", "invalid"))
    }
  }

  def checkAllValuesOfArray[A](arr: JsonArray, p: (A => Boolean), name: String): ArgumentCheck[JsonArray] = {
    import scala.collection.JavaConverters._
    val tail = arr.asScala.dropWhile(value => Try(p(value.asInstanceOf[A])).getOrElse(false))

    if (tail.isEmpty) {
      OkArg(arr)
    } else {
      FailArg(InvalidJsonException(s"Warning: $name has incorrectly typed value or value is wrong.", "invalid"))
    }
  }

  def sequence[A](argChecks: Seq[ArgumentCheck[A]]): ArgumentCheck[Seq[A]] = {
    argChecks match {
      case Nil => OkArg(Nil)
      case argCheck :: t => argCheck flatMap (a => sequence(t) map (a +: _))
    }
  }

  def tryMap[A, B](tryFn: A => B, ex: => CustomException)(a: A): ArgumentCheck[B] = {
    Try(OkArg(tryFn(a))).getOrElse(FailArg[B](ex))
  }

  def originTablesCheck[A](tableType: A, originTables: Option[Seq[_]]): ArgumentCheck[Unit] = {
    (tableType == UnionTable && originTables.isDefined) || (tableType != UnionTable && originTables.isEmpty) match {
      case true => OkArg(())
      case false =>
        FailArg(
          UnprocessableEntityException(
            s"originTables can only be provided when tableType is UnionTable"
          )
        )
    }
  }

  def checkArguments(args: ArgumentCheck[_]*): Unit = {
    val failedArgs: Vector[String] = args.zipWithIndex.foldLeft(Vector[String]()) {
      case (errors, (FailArg(ex), idx)) => errors :+ s"($idx) ${ex.message}"
      case (errors, (OkArg(x), idx)) => errors
    }

    if (failedArgs.nonEmpty) throw new IllegalArgumentException(failedArgs.mkString("\n"))
  }

  def checked[A](checkedArgument: ArgumentCheck[A]): A = checkedArgument.get
}
