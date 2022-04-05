package com.campudus.tableaux.helper

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.campudus.tableaux.controller.SystemController
import org.vertx.scala.core.json.{Json, JsonObject, JsonArray}
import com.campudus.tableaux.database.domain.{
  DateColumn,
  DateTimeColumn,
  DomainObject,
  NumberColumn,
  BooleanColumn,
  LinkColumn,
  ColumnType
}
import com.campudus.tableaux.database.MultiLanguage
import com.campudus.tableaux.database.domain.ConcatColumn
import com.campudus.tableaux.database.domain.GroupColumn
import scala.collection.JavaConverters._

class DisplayValues(langtags: Seq[String]) {
  def getDefaultLangtag = { langtags.head }

  def getColumnDisplayNameWithFallback(column: ColumnType[_], lt: String): String = {
    val displayNames = column.getJson.getJsonObject("displayName")
    displayNames.getString(lt, displayNames.getString(getDefaultLangtag, column.name))
  }

  def getDisplayValue(column: ColumnType[_])(value: Any): Any = {
    println(column.kind)
    val displayValue = column match {
      case col: ConcatColumn => getConcatValue(col)(value)
      case col: LinkColumn => getLinkValue(col)(value)
      case BooleanColumn(_) => getBoolValue(column)(value.asInstanceOf[Boolean])
      case NumberColumn(_) => getNumericValue(column)(value)
      case DateColumn(_) => getDateValue(column)(value)
      case DateTimeColumn(_) => getDateValue(column)(value)
      case _ => getTextValue(column)(value)
    }
    displayValue
  }

  private def getTextValue(column: ColumnType[_])(value: Any): JsonObject = {
    val getValue = (lt: String) => {
      value match {
        case s: String => s
        case o: JsonObject => o.getString(lt, "")
        case _ => ""
      }
    }
    applyToAllLangs(getValue)
  }

  private def flatten(seq: Seq[_]): Seq[_] = {
    var sequence: Seq[Any] = Seq()
    seq.foreach(value => {
      value match {
        case j: JsonArray => sequence = sequence ++ j.getList().asScala.asInstanceOf[Seq[_]]
        case s: Seq[_] => sequence = sequence ++ s
        case a: Any => sequence = sequence :+ a
      }
    })
    sequence
  }

  private def getConcatValue(column: ColumnType[_])(value: Any): JsonArray = {
    // println(value)
    val columns = column match {
      case col: ConcatColumn => col.columns
      case col: GroupColumn => col.columns
      case _ => Seq()
    }
    val zipped: Seq[(ColumnType[_], Any)] = columns zip value
      .asInstanceOf[JsonArray]
      .getList()
      .asScala
    // println(zipped)
    val res = zipped
      .map({
        case (col: ColumnType[_], value: Any) => getDisplayValue(col)(value)
      })
    // println(res.toString)
    // println(flatten(res))
    val returnValue = Json.arr(flatten(res))
    // println(returnValue)
    returnValue
  }

  private def getLinkValue(column: LinkColumn)(value: Any): JsonArray = {
    val linkValues: Seq[JsonObject] = value match {
      case l: Seq[_] => l.map(obj => obj.asInstanceOf[JsonObject])
      case _ => Seq()
    }
    // println(column.to)
    val res = linkValues.map(linkValue => getDisplayValue(column.to)(linkValue.getValue("value")))
    // println(flatten(res))
    Json.arr(flatten(res))
  }

  private def placeholder(column: ColumnType[_])(value: Any): String = { "placeholder" }

  private def getDateValue(column: ColumnType[_])(value: Any): JsonObject = {
    val getValue = (lt: String) => {
      val date = value match {
        case v: JsonObject => v.getString(lt, "")
        case s: String => s
        case _ => ""
      }
      val formattedDate = date.isEmpty() match {
        case true => ""
        case false => {
          val formatString = column match {
            case DateTimeColumn(_) => "dd.MM.yyyy - hh:mm"
            case _ => "dd.MM.yyyy"
          }
          val formatter = DateTimeFormat.forPattern(formatString)
          formatter.print(DateTime.parse(date))
        }
      }
      formattedDate
    }
    applyToAllLangs(getValue)
  }

  private def getBoolValue(column: ColumnType[_])(value: Any): JsonObject = {
    val getValue = (lt: String) => {
      val bool = value match {
        case b: Boolean => b
        case o: JsonObject => o.getBoolean(lt, false)
        case _ => false
      }
      bool match {
        case true => getColumnDisplayNameWithFallback(column, lt)
        case false => ""
      }
    }
    applyToAllLangs(getValue)
  }

  private def getNumericValue(column: ColumnType[_])(value: Any): JsonObject = {
    val getValue = (lt: String) => {
      value match {
        case n: Number => n.toString
        case o: JsonObject => { o.getValue(lt, "").toString }
        case _ => { "" }
      }
    }
    applyToAllLangs(getValue)
  }

  private def getDefaultValue(column: ColumnType[_])(value: Any): String = { "test" }

  private def applyToAllLangs(func: String => Any): JsonObject = {
    langtags.foldLeft(Json.obj()) { (acc: JsonObject, lt: String) =>
      acc.put(lt, func(lt))
    }
  }
}
