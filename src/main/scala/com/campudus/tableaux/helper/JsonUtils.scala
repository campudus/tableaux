package com.campudus.tableaux.helper

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering}
import com.campudus.tableaux.{ArgumentCheck, FailArg, InvalidJsonException, OkArg}
import com.typesafe.scalalogging.LazyLogging
import org.vertx.scala.core.json.{JsonArray, JsonObject}

import scala.util.Try

object JsonUtils extends LazyLogging {

  private def asCastedList[A](array: JsonArray): ArgumentCheck[Seq[A]] = {
    import scala.collection.JavaConverters._
    sequence(array.asScala.toList.map(notNull(_, "some array value").flatMap(tryCast[A])))
  }

  private def checkNotNullArray(json: JsonObject, field: String): ArgumentCheck[JsonArray] = notNull(json.getJsonArray(field), field)

  private def checkForJsonObject(seq: Seq[JsonObject]): ArgumentCheck[Seq[JsonObject]] = {
    tryMap((y: Seq[JsonObject]) => y map { x: JsonObject => x }, InvalidJsonException(s"Warning: Array should only contain JsonObjects", "object"))(seq)
  }

  private def toTableauxType(kind: String): ArgumentCheck[TableauxDbType] = {
    tryMap(TableauxDbType.apply, InvalidJsonException("Warning: No such type", "type"))(kind)
  }

  private def toJsonObjectSeq(field: String, json: JsonObject): ArgumentCheck[Seq[JsonObject]] = {
    for {
      jsonArray <- checkNotNullArray(json, field)
      jsonObjectList <- asCastedList[JsonObject](jsonArray)
      nonEmptyJsonObjectList <- nonEmpty(jsonObjectList, field)
      checkedNonEmptyJsonObjectList <- checkForJsonObject(nonEmptyJsonObjectList)
    } yield checkedNonEmptyJsonObjectList
  }

  def toCreateColumnSeq(json: JsonObject): Seq[CreateColumn] = (for {
    seq <- toJsonObjectSeq("columns", json)
    tuples <- sequence(seq map {
      json =>
        for {
        // required fields
          name <- notNull(json.getString("name"), "name")
          kind <- notNull(json.getString("kind"), "kind")

          dbType <- toTableauxType(kind)
        } yield {
          // optional fields
          val ordering = Try(json.getInteger("ordering").longValue()).toOption
          val identifier = Try[Boolean](json.getBoolean("identifier")).getOrElse(false)

          // languageType or deprecated multilanguage
          // if languageType == 'country' countryCodes must be specified
          val languageType = parseJsonForLanguageType(json)

          // displayName and description; both multi-language objects
          val displayInfos = DisplayInfos.allInfos(json)

          dbType match {
            case AttachmentType =>
              CreateAttachmentColumn(name, ordering, identifier, displayInfos)

            case LinkType =>
              // link specific fields
              val toName = Try(Option(json.getString("toName"))).toOption.flatten
              val singleDirection = Try[Boolean](json.getBoolean("singleDirection")).getOrElse(false)
              val toTableId = notNull(json.getLong("toTable").toLong, "toTable").get

              CreateLinkColumn(name, ordering, toTableId, toName, singleDirection, identifier, displayInfos)

            case _ =>
              CreateSimpleColumn(name, ordering, dbType, languageType, identifier, displayInfos)
          }
        }
    })
  } yield tuples).get

  private def parseJsonForLanguageType(json: JsonObject): LanguageType = {
    if (json.containsKey("languageType")) {
      json.getString("languageType") match {
        case LanguageType.LANGUAGE => MultiLanguage
        case LanguageType.COUNTRY =>
          if (json.containsKey("countryCodes")) {
            import scala.collection.JavaConverters._

            val countryCodeSeq = checkAllValuesOfArray[String](json.getJsonArray("countryCodes"),
              d => d.isInstanceOf[String] && d.matches("[A-Z]{2,3}"), "countryCodes")
              .map(_.asScala.toSeq.map({ case code: String => code }))
              .get

            MultiCountry(CountryCodes(countryCodeSeq))
          } else {
            throw new InvalidJsonException("If 'languageType' is 'country' the field 'countryCodes' must be specified.", "countrycodes")
          }
        case LanguageType.NEUTRAL => LanguageNeutral
        case _ => throw new InvalidJsonException("Field 'languageType' should only contain 'neutral', 'language' or 'country'", "languagetype")
      }
    } else if (json.containsKey("multilanguage")) {
      logger.warn("JSON contains deprecated field 'multilanguage' use 'languageType' instead.")

      if (json.getBoolean("multilanguage")) {
        MultiLanguage
      } else {
        LanguageNeutral
      }
    } else {
      LanguageNeutral
    }
  }

  def toRowValueSeq(json: JsonObject): Seq[Seq[_]] = (for {
    checkedRowList <- toJsonObjectSeq("rows", json)
    result <- sequence(checkedRowList map toValueSeq)
  } yield result).get

  def toColumnValueSeq(json: JsonObject): Seq[Seq[(ColumnId, _)]] = (for {
    columnsObject <- toJsonObjectSeq("columns", json)
    columns = sequence(columnsObject.map(hasLong("id", _)))
    rows <- toJsonObjectSeq("rows", json)
    result <- mergeColumnWithValue(columns.get, rows)
  } yield result).get

  private def mergeColumnWithValue(columns: Seq[ColumnId], rows: Seq[JsonObject]): ArgumentCheck[Seq[Seq[(ColumnId, _)]]] = {
    sequence(rows map { row =>
      toValueSeq(row) flatMap { values =>
        checkSameLengthsAndZip[ColumnId, Any](columns, values)
      }
    })
  }

  private def toValueSeq(json: JsonObject): ArgumentCheck[Seq[Any]] = for {
    values <- checkNotNullArray(json, "values")
    valueAsAnyList <- asCastedList[Any](values)
    valueList <- nonEmpty(valueAsAnyList, "values")
  } yield valueList

  def toColumnChanges(json: JsonObject): (Option[String], Option[Ordering], Option[TableauxDbType], Option[Boolean], Option[JsonObject], Option[JsonObject], Option[Seq[String]]) = {
    import scala.collection.JavaConverters._

    val name = Try(notNull(json.getString("name"), "name").get).toOption
    val ord = Try(json.getInteger("ordering").longValue()).toOption
    val kind = Try(toTableauxType(json.getString("kind")).get).toOption
    val identifier = Try(json.getBoolean("identifier").booleanValue()).toOption
    val displayNames = Try(checkForAllValues[String](json.getJsonObject("displayName"), n => n == null || n.isInstanceOf[String], "displayName").get).toOption
    val descriptions = Try(checkForAllValues[String](json.getJsonObject("description"), d => d == null || d.isInstanceOf[String], "description").get).toOption

    val countryCodes = booleanToValueOption(json.containsKey("countryCodes"), {
      checkAllValuesOfArray[String](json.getJsonArray("countryCodes"), d => d.isInstanceOf[String] && d.matches("[A-Z]{2,3}"), "countryCodes").get
    }).map(_.asScala.toSeq.map({ case code: String => code }))

    (name, ord, kind, identifier, displayNames, descriptions, countryCodes)
  }

  def booleanToValueOption[A](boolean: Boolean, value: => A): Option[A] = {
    if (boolean) {
      Some(value)
    } else {
      None
    }
  }

  def toLocationType(json: JsonObject): LocationType = {
    (for {
      location <- notNull(json.getString("location"), "location")
      location <- oneOf(location, List("start", "end", "before"), "location")
    } yield {
      val relativeTo = if ("before" == location) {
        isDefined(Option(json.getLong("id")).map(_.toLong), "id") match {
          case FailArg(ex) => throw ex
          case OkArg(id) => Some(id)
        }
      } else {
        None
      }

      LocationType(location, relativeTo)
    }).get
  }
}
