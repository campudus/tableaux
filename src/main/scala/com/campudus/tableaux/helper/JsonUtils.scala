package com.campudus.tableaux.helper

import com.campudus.tableaux.ArgumentChecker._
import com.campudus.tableaux.database._
import com.campudus.tableaux.database.domain._
import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, Ordering}
import com.campudus.tableaux.{ArgumentCheck, FailArg, InvalidJsonException, OkArg}
import com.typesafe.scalalogging.LazyLogging
import org.vertx.scala.core.json.{Json, JsonArray, JsonObject}
import com.campudus.tableaux.{InvalidJsonException, WrongJsonTypeException, TableauxConfig}
import com.campudus.tableaux.verticles.JsonSchemaValidator.JsonSchemaValidatorClient
import io.vertx.scala.core.Vertx

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object JsonUtils extends LazyLogging {

  def asCastedNullableList[A](array: JsonArray): ArgumentCheck[Seq[A]] = {
    Option(array)
      .map({ array =>
        {
          val arrayAsList = array.asScala.toList

          sequence(arrayAsList.map(tryCast[A]))
        }
      })
      .getOrElse(OkArg(Seq.empty))
  }

  def asCastedList[A](array: JsonArray): ArgumentCheck[Seq[A]] = {
    Option(array)
      .map({ array =>
        {

          val arrayAsList = array.asScala.toList.zipWithIndex

          sequence(
            arrayAsList
              .map({
                case (value, index) =>
                  notNull(value, s"value at index $index in array with length ${array.size()}")
                    .flatMap(tryCast[A])
              })
          )
        }
      })
      .getOrElse(OkArg(Seq.empty))
  }

  private def checkNotNullArray(json: JsonObject, field: String): ArgumentCheck[JsonArray] = {
    notNull(json.getJsonArray(field), field)
  }

  private def checkForJsonObject(seq: Seq[JsonObject]): ArgumentCheck[Seq[JsonObject]] = {
    tryMap((y: Seq[JsonObject]) => {
      y map { x: JsonObject =>
        {
          x
        }
      }
    }, InvalidJsonException(s"Warning: Array should only contain JsonObjects", "object"))(seq)
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

  def toCreateColumnSeq(json: JsonObject): Seq[CreateColumn] = {
    (for {
      columnObjects <- toJsonObjectSeq("columns", json)
      createColumnSeq <- sequence(columnObjects.map({ json =>
        {
          for {
            // required fields
            name <- notNull(json.getString("name"), "name")
            kind <- notNull(json.getString("kind"), "kind")

            dbType <- toTableauxType(kind)
          } yield {
            // optional fields
            val ordering = Try(json.getInteger("ordering").longValue()).toOption
            val identifier = json.getBoolean("identifier", false)
            val separator = json.getBoolean("separator", true)
            val attributes = Try(json.getJsonObject("attributes")) match {
              case Success(value) => Option(value)
              case Failure(s) => throw WrongJsonTypeException("Field attributes is not a valid json object.")
            }

            // languageType or deprecated multilanguage
            // if languageType == 'country' countryCodes must be specified
            val languageType = parseJsonForLanguageType(json)

            // displayName and description; both multi-language objects
            val displayInfos = DisplayInfos.fromJson(json)

            dbType match {
              case AttachmentType =>
                CreateAttachmentColumn(name, ordering, identifier, displayInfos, attributes)

              case LinkType =>
                // link specific fields
                val singleDirection = Try[Boolean](json.getBoolean("singleDirection")).getOrElse(false)
                val toTableId = hasLong("toTable", json).get

                // bi-directional information for foreign link column (backlink)
                val toName = Try(Option(json.getString("toName"))).toOption.flatten
                val toDisplayInfos =
                  Try(Option(json.getJsonObject("toDisplayInfos"))).toOption.flatten.map(DisplayInfos.fromJson)
                val toOrdering = hasLong("toOrdering", json).toOption

                val createBackLinkColumn = CreateBackLinkColumn(
                  name = toName,
                  displayInfos = toDisplayInfos,
                  ordering = toOrdering
                )

                // constraints = cardinality and/or deleteCascade
                val constraint = for {
                  (cardinalityFrom, cardinalityTo) <- Try[(Int, Int)]({
                    val cardinality = json
                      .getJsonObject("constraint")
                      .getJsonObject("cardinality", new JsonObject())

                    (cardinality.getInteger("from", 0).intValue(), cardinality.getInteger("to", 0).intValue())
                  }).orElse(Success((0, 0)))

                  deleteCascade <- Try[Boolean](
                    json
                      .getJsonObject("constraint")
                      .getBoolean("deleteCascade")
                  ).orElse(Success(false))
                } yield Constraint(Cardinality(cardinalityFrom, cardinalityTo), deleteCascade)

                CreateLinkColumn(name,
                                 ordering,
                                 toTableId,
                                 singleDirection,
                                 identifier,
                                 displayInfos,
                                 constraint.getOrElse(DefaultConstraint),
                                 createBackLinkColumn,
                                 attributes)

              case GroupType =>
                // group specific fields

                val groups = checked(hasArray("groups", json)).asScala
                  .map(_.asInstanceOf[Int])
                  .map(_.toLong)
                  .toSeq

                val formatPattern = Try(Option(json.getString("formatPattern"))).toOption.flatten

                CreateGroupColumn(name, ordering, identifier, formatPattern, displayInfos, groups, attributes)

              case StatusType =>

                val validator = JsonSchemaValidatorClient(Vertx.currentContext().get.owner())
                val rules = json.getJsonArray("rules", new JsonArray())
                       CreateStatusColumn(
                         name,
                         ordering,
                         dbType,
                         displayInfos,
                         attributes,
                         rules
                         )


              case _ =>
                CreateSimpleColumn(name,
                                   ordering,
                                   dbType,
                                   languageType,
                                   identifier,
                                   displayInfos,
                                   separator,
                                   attributes)
            }
          }
        }
      }))
    } yield createColumnSeq).get
  }

  private def parseJsonForLanguageType(json: JsonObject): LanguageType = {
    if (json.containsKey("languageType")) {
      json.getString("languageType") match {
        case LanguageType.LANGUAGE => MultiLanguage
        case LanguageType.COUNTRY =>
          if (json.containsKey("countryCodes")) {

            val countryCodeSeq = checkAllValuesOfArray[String](json.getJsonArray("countryCodes"),
                                                               d => d.isInstanceOf[String] && d.matches("[A-Z]{2,3}"),
                                                               "countryCodes")
              .map(_.asScala.toSeq.map({ case code: String => code }))
              .get

            MultiCountry(CountryCodes(countryCodeSeq))
          } else {
            throw InvalidJsonException("If 'languageType' is 'country' the field 'countryCodes' must be specified.",
                                       "countrycodes")
          }
        case LanguageType.NEUTRAL => LanguageNeutral
        case _ =>
          throw InvalidJsonException("Field 'languageType' should only contain 'neutral', 'language' or 'country'",
                                     "languagetype")
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

  def toRowValueSeq(json: JsonObject): Seq[Seq[_]] = {
    (for {
      checkedRowList <- toJsonObjectSeq("rows", json)
      result <- sequence(checkedRowList map toValueSeq)
    } yield result).get
  }

  def toColumnValueSeq(json: JsonObject): Seq[Seq[(ColumnId, _)]] = {
    (for {
      columnsObject <- toJsonObjectSeq("columns", json)
      columns = sequence(columnsObject.map(hasLong("id", _)))
      rows <- toJsonObjectSeq("rows", json)
      result <- mergeColumnWithValue(columns.get, rows)
    } yield result).get
  }

  private def mergeColumnWithValue(
      columns: Seq[ColumnId],
      rows: Seq[JsonObject]
  ): ArgumentCheck[Seq[Seq[(ColumnId, _)]]] = {
    sequence(rows map { row =>
      toValueSeq(row) flatMap { values =>
        checkSameLengthsAndZip[ColumnId, Any](columns, values)
      }
    })
  }

  private def toValueSeq(json: JsonObject): ArgumentCheck[Seq[Any]] = {
    for {
      values <- checkNotNullArray(json, "values")
      valueAsAnyList <- asCastedNullableList[Any](values)
      valueList <- nonEmpty(valueAsAnyList, "values")
    } yield valueList
  }

  def toColumnChanges(json: JsonObject): (Option[String],
                                          Option[Ordering],
                                          Option[TableauxDbType],
                                          Option[Boolean],
                                          Option[Seq[DisplayInfo]],
                                          Option[Seq[String]],
                                          Option[Boolean],
                                          Option[JsonObject]) = {

    val name = Try(notNull(json.getString("name"), "name").get).toOption
    val ord = Try(json.getInteger("ordering").longValue()).toOption
    val kind = Try(toTableauxType(json.getString("kind")).get).toOption
    val identifier = Try(json.getBoolean("identifier").booleanValue()).toOption
    val separator = Try(json.getBoolean("separator").booleanValue()).toOption
    val attributes = Try(json.getJsonObject("attributes")) match {
      case Success(value) => Option(value)
      case Failure(s) => throw WrongJsonTypeException("Field attributes is not a valid json object.")
    }
    val displayInfos = DisplayInfos.fromJson(json) match {
      case list if list.isEmpty => None
      case list => Some(list)
    }

    val countryCodes = booleanToValueOption(
      json.containsKey("countryCodes"), {
        checkAllValuesOfArray[String](json.getJsonArray("countryCodes"),
                                      d => d.isInstanceOf[String] && d.matches("[A-Z]{2,3}"),
                                      "countryCodes").get
      }
    ).map(_.asScala.toSeq.map({ case code: String => code }))

    (name, ord, kind, identifier, displayInfos, countryCodes, separator, attributes)
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
        isDefined(Option(json.getLong("id")).map(_.longValue()), "id") match {
          case FailArg(ex) => throw ex
          case OkArg(id) => Some(id)
        }
      } else {
        None
      }

      LocationType(location, relativeTo)
    }).get
  }

  def parseJson(jsonStringOpt: String): JsonObject = {
    Option(jsonStringOpt) match {
      case Some(jsonString) =>
        Try(Json.fromObjectString(jsonString)) match {
          case Success(json) => json
          case Failure(_) =>
            logger.error(s"Couldn't parse json. Expected JSON but got: $jsonString")
            Json.emptyObj()
        }
      case None => Json.emptyObj()
    }
  }

  /**
    * Helper to cast a Json Array to a scala Seq of class A
    *
    * @param jsonArray
    * @tparam A
    * @return Seq[A]
    */
  def asSeqOf[A](jsonArray: JsonArray): Seq[A] = {
    jsonArray.asScala.map(_.asInstanceOf[A]).toSeq
  }

}
