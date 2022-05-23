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
import scala.util.matching.Regex
import com.campudus.tableaux.database.MultiLanguage
import com.campudus.tableaux.database.domain.ConcatColumn
import com.campudus.tableaux.database.domain.GroupColumn
import scala.collection.JavaConverters._
import com.campudus.tableaux.database.domain.CurrencyColumn
import com.campudus.tableaux.database.domain.AttachmentColumn
import com.campudus.tableaux.database.model.AttachmentFile

class DisplayValues(langtagsString: String) {
  def getDefaultLangtag = { langtags.head }
  val langtags = jsonArrayToStringSeq(new JsonArray(langtagsString))

  def getColumnDisplayNameWithFallback(column: ColumnType[_], lt: String): String = {
    val displayNames = column.getJson.getJsonObject("displayName")
    displayNames.getString(lt, displayNames.getString(getDefaultLangtag, column.name))
  }

  def getDisplayValue(column: ColumnType[_])(value: Any): Any = {
    val displayValue = column match {
      case col: GroupColumn => getConcatValue(col)(value)
      case col: ConcatColumn => getConcatValue(col)(value)
      case col: LinkColumn => getLinkValue(col)(value)
      case BooleanColumn(_) => getBoolValue(column)(value.asInstanceOf[Boolean])
      case NumberColumn(_) => getNumericValue(column)(value)
      case DateColumn(_) => getDateValue(column)(value)
      case DateTimeColumn(_) => getDateValue(column)(value)
      case col: CurrencyColumn => getCurrencyValue(col)(value)
      case AttachmentColumn(_) => getAttachmentFileName(value)
      case _ => getTextValue(column)(value)
    }
    displayValue
  }

  private def getAttachmentFileName(value: Any): JsonArray = {

    def getValue(link: JsonObject)(lt: String) = {

      val getLangValueFromAttachment: ((String, String)) => String = {
        case (objectKey, lt) => {
          link.getJsonObject(objectKey, Json.obj()).getString(lt, "")
        }
      }

      val names = Seq(
        ("title", lt),
        ("externalName", lt),
        ("internalName", lt),
        ("title", getDefaultLangtag),
        ("externalName", getDefaultLangtag),
        ("internalName", getDefaultLangtag)
      )
      val nameOption = names.map(getLangValueFromAttachment).filter(str => !str.isEmpty()).headOption

      nameOption match {
        case None => "unnamed file"
        case Some(name) => name
      }

    }

    val links: Seq[JsonObject] = value match {
      case arr: Seq[_] => arr.map(link => link.asInstanceOf[AttachmentFile].getJson)
      case _ => Seq()
    }
    new JsonArray(links.map(link => applyToAllLangs(getValue(link))).asJava)

  }

  private def getCountryOfLangtag(lt: String): String = {
    val splitLt = "[-_]".r.split(lt)
    splitLt.length > 1 match {
      case true => splitLt(1)
      case false => {
        val firstPart = splitLt(0).asInstanceOf[String]
        firstPart.toUpperCase()
      }
    }
  }

  val currencyCodeMap = Map(
    "AE" -> "AED", // United Arab Emirates
    "AT" -> "EUR", // Austria
    "BE" -> "EUR", // Belgium
    "BG" -> "BGN", // Bulgaria
    "BR" -> "BRL", // Brazil
    "CA" -> "CAD", // Canada
    "CH" -> "SFR", // Switzerland
    "CN" -> "CNY", // China
    "CZ" -> "CZK", // Czechia
    "DE" -> "EUR", // Germany
    "DK" -> "DKK", // Denmark
    "ES" -> "EUR", // Spain
    "FI" -> "EUR", // Finland
    "FR" -> "EUR", // France
    "GB" -> "GBP", // Great Britain
    "GR" -> "EUR", // Greece
    "HK" -> "HKD", // Hong Kong
    "HR" -> "HRK", // Croatia
    "HU" -> "HUF", // Hungary
    "ID" -> "IDR", // Indonesia
    "IE" -> "EUR", // Ireland
    "IL" -> "ILS", // Israel
    "IN" -> "INR", // India
    "IQ" -> "IQD", // Iraq
    "IT" -> "EUR", // Italy
    "JP" -> "JPY", // Japan
    "KR" -> "KRW", // Korea South
    "KW" -> "KWD", // Kuwait
    "LI" -> "CHF", // Liechtenstein
    "LU" -> "EUR", // Luxembourg
    "MA" -> "MAD", // Morocco
    "MC" -> "EUR", // Monaco
    "ME" -> "EUR", // Montenegro
    "MX" -> "MXN", // Mexico
    "NL" -> "EUR", // Netherlands
    "NO" -> "NOK", // Norway
    "NZ" -> "NZD", // New Zealand
    "PL" -> "PLN", // Poland
    "PT" -> "EUR", // Portugal
    "RO" -> "RON", // Romania
    "RS" -> "RSD", // Serbia
    "RU" -> "RUB", // Russian Federation
    "SA" -> "SAR", // Saudi Arabia
    "SE" -> "SEK", // Sweden
    "SG" -> "SGD", // Singapore
    "SI" -> "EUR", // Slovenia
    "TH" -> "THB", // Thailand
    "TR" -> "TRY", // Turkey
    "TW" -> "TWD", // Taiwan
    "UA" -> "UAH", // Ukraine
    "US" -> "USD", // United States of America
    "ZA" -> "ZAR" // South Africa
  )

  val reverseCurrencyCodeMap = (() => {
    currencyCodeMap.keys.foldLeft(Map[String, Seq[String]]())((acc, value) => {
      currencyCodeMap get value match {
        case None => acc
        case Some(currencyCode) => {
          acc get currencyCode match {
            case None => {
              acc + (currencyCode -> Seq(value))
            }
            case Some(currencyCodeArr) => {
              acc + (currencyCode -> (currencyCodeArr ++ Seq(value)))
            }
          }
        }
      }
    })
  })()

  private def getFallbackCurrencyValue(country: String, value: JsonObject): String = {
    val _country = getCountryOfLangtag(country)
    val currency = currencyCodeMap.getOrElse(_country, "")
    val fallbackEntry = reverseCurrencyCodeMap
      .getOrElse(currency, Seq())
      .filter((value) => { value != _country })
      .find(ctry => !value.getValue(ctry, "").toString.isEmpty || value.getValue(ctry, "").isInstanceOf[Number])
    fallbackEntry match {
      case None => ""
      case Some(entry) => value.getValue(entry, "").toString
    }
  }

  private def getCurrencyWithCountry(value: JsonObject, country: String): String = {
    value.getValue(country, getFallbackCurrencyValue(country, value)).toString
  }

  private def getCurrencyValue(col: CurrencyColumn)(value: Any): JsonObject = {
    val getValue = (lt: String) => {
      val country = getCountryOfLangtag(lt)
      val rawValue: String = getCurrencyWithCountry(value.asInstanceOf[JsonObject], country)
      val currencyCode: String = currencyCodeMap.get(country) match {
        case Some(value) => value
        case None => ""
      }

      rawValue.isEmpty() match {
        case true => ""
        case false => rawValue.replace(".", ",") + " " + currencyCode
      }
    }
    applyToAllLangs(getValue)
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

  private def flatten(seq: Seq[_]): Seq[JsonObject] = {
    var sequence: Seq[Any] = Seq()
    seq.foreach(value => {
      value match {
        case j: JsonArray => sequence = sequence ++ j.getList().asScala.asInstanceOf[Seq[_]]
        case s: Seq[_] => sequence = sequence ++ s
        case a: Any => sequence = sequence :+ a
      }
    })
    sequence.map({
      case o: JsonObject => o
    })
  }

  private def moustache(n: Number) = {
    val str = s"\\{\\{${n}\\}\\}"
    str.r
  }

  private def getConcatValue(column: ColumnType[_])(value: Any): JsonObject = {
    val columns: Seq[ColumnType[_]] = column match {
      case col: ConcatColumn => col.columns
      case col: GroupColumn => col.columns
      case _ => Seq()
    }

    def _flatten(something: Seq[_]) = {
      var stuff: Seq[_] = Seq()
      something.foreach(anything => {
        anything match {
          case j: JsonArray => stuff = stuff ++ j.getList().asScala.asInstanceOf[Seq[_]]
          case s: Seq[_] => stuff = stuff ++ s
          case a: Any => stuff = stuff :+ a
        }
      })
      stuff
    }
    val _value: Seq[_] = value match {
      case col: Seq[_] => col
      case jsonArr: JsonArray => jsonArr.getList().asScala
    }
    val zipped: Seq[(ColumnType[_], Any)] = columns zip _value

    val displayValues: Seq[JsonObject] = flatten(zipped.toList.map({
      case (col: ColumnType[_], value) => {
        val displayValue = getDisplayValue(col)(value)
        col match {
          case l: LinkColumn => displayValue.asInstanceOf[JsonArray]
          case _ => displayValue.asInstanceOf[JsonObject]
        }
      }

    }))
    val format = (valArray: Seq[String]) => {
      val simpleConcat = () => valArray.map(_.trim).filter(str => !str.isEmpty()).mkString(" ").trim()

      def getColumnIdForIndex(index: Int) = {
        columns.asInstanceOf[IndexedSeq[ColumnType[_]]](index - 1).id
      }
      def applyFormat(result: String, dVal: Seq[String] = valArray, i: Int = 1): String = {
        dVal.isEmpty match {
          case true => result
          case false => {
            applyFormat(moustache(getColumnIdForIndex(i)).replaceAllIn(result, dVal.head.trim()), dVal.tail, i + 1)
          }
        }
      }
      column match {
        case col: GroupColumn => {
          col.formatPattern match {
            case None => {
              simpleConcat()
            }
            case Some(formatPattern) => applyFormat(formatPattern)
          }
        }
        case _ => simpleConcat()
      }
    }
    val obj = applyToAllLangs(lt => format(displayValues.map(obj => obj.getString(lt, ""))))
    obj
  }

  private def jsonArrayToSeq(arr: JsonArray): Seq[JsonObject] = {
    arr.stream().iterator().asScala.toList.map(obj => obj.asInstanceOf[JsonObject])
  }

  private def jsonArrayToStringSeq(arr: JsonArray): Seq[String] = {
    arr.stream().iterator().asScala.toList.map(obj => obj.asInstanceOf[String])
  }

  private def getLinkValue(column: LinkColumn)(value: Any): JsonArray = {

    val linkValues: Seq[JsonObject] = value match {
      case l: Seq[_] => l.map(obj => obj.asInstanceOf[JsonObject])
      case a: JsonArray => jsonArrayToSeq(a)
      case _ => Seq()
    }
    val res = linkValues.map(linkValue => {
      getDisplayValue(column.to)(linkValue.getValue("value"))
    })
    new JsonArray(res.asJava)
  }

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

  private def applyToAllLangs(func: String => Any): JsonObject = {
    langtags.foldLeft(Json.obj()) { (acc: JsonObject, lt: String) =>
      acc.put(lt, func(lt))
    }
  }
}
