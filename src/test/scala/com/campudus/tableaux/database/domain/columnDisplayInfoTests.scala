package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import com.campudus.tableaux.testtools.TestAssertionHelper
import org.junit.Assert._
import org.junit.Test
import org.vertx.scala.core.json.Json

import scala.util.{Failure, Success, Try}

abstract class AbstractColumnDisplayInfosTest extends TestAssertionHelper {

  @Test
  def checkSingleName(): Unit = {
    val di = singleName(1, 1, "de-DE", "Spalte 1")
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    assertEquals(Seq(1, 1, "de-DE", "Spalte 1", null), binds)
  }

  @Test
  def checkMultipleNames(): Unit = {
    val di = multipleNames(1, 1, List("de-DE" -> "Spalte 1", "en-GB" -> "Column 1"))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    checkPartsInRandomOrder(
      Seq(
        Seq(1, 1, "de-DE", "Spalte 1", null),
        Seq(1, 1, "en-GB", "Column 1", null)
      ),
      binds
    )
  }

  @Test
  def checkSingleDescription(): Unit = {
    val di = singleDesc(1, 1, "de-DE", "Spalte 1 Beschreibung")
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    assertEquals(Seq(1, 1, "de-DE", null, "Spalte 1 Beschreibung"), binds)
  }

  @Test
  def checkMultipleDescriptions(): Unit = {
    val di = multipleDescs(1, 1, List("de-DE" -> "Spalte 1 Beschreibung", "en-GB" -> "Column 1 Description"))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    checkPartsInRandomOrder(
      Seq(
        Seq(1, 1, "de-DE", null, "Spalte 1 Beschreibung"),
        Seq(1, 1, "en-GB", null, "Column 1 Description")
      ),
      binds
    )
  }

  @Test
  def checkSingleNameAndDescription(): Unit = {
    val di = singleNameAndDesc(1, 1, "de-DE", "Spalte 1", "Spalte 1 Beschreibung")
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    assertEquals(Seq(1, 1, "de-DE", "Spalte 1", "Spalte 1 Beschreibung"), binds)
  }

  @Test
  def checkMultipleNamesAndDescriptions(): Unit = {
    val di = multipleNameAndDesc(
      1,
      1,
      List(("de-DE", "Spalte 1", "Spalte 1 Beschreibung"), ("en-GB", "Column 1", "Column 1 Description"))
    )
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    checkPartsInRandomOrder(
      Seq(
        Seq(1, 1, "de-DE", "Spalte 1", "Spalte 1 Beschreibung"),
        Seq(1, 1, "en-GB", "Column 1", "Column 1 Description")
      ),
      binds
    )
  }

  @Test
  def checkNameAndOtherDesc(): Unit = {
    val di = multipleNameAndDesc(1, 1, List(("de-DE", "Spalte 1", null), ("en-GB", null, "Column 1 Description")))
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    checkPartsInRandomOrder(
      Seq(
        Seq(1, 1, "de-DE", "Spalte 1", null),
        Seq(1, 1, "en-GB", null, "Column 1 Description")
      ),
      binds
    )
  }

  @Test
  def checkCombinations(): Unit = {
    val di = multipleNameAndDesc(
      1,
      1,
      List(
        ("de-DE", "Spalte 1", "Spalte 1 Beschreibung"),
        ("en-GB", null, "Column 1 Description"),
        ("fr_FR", "Colonne 1", null)
      )
    )
    val (statement, binds) = di.createSql
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
         |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin.replaceAll("\n", " "),
      statement
    )
    val all = Seq(
      Seq(1, 1, "de-DE", "Spalte 1", "Spalte 1 Beschreibung"),
      Seq(1, 1, "en-GB", null, "Column 1 Description"),
      Seq(1, 1, "fr_FR", "Colonne 1", null)
    )

    checkPartsInRandomOrder(all, binds)
  }

  @Test
  def emptyDisplayStuff(): Unit = {
    val di = emptyDisplayInfo(1, 1)
    assertFalse(di.nonEmpty)
  }

  def emptyDisplayInfo(tableId: TableId, columnId: ColumnId): ColumnDisplayInfos

  def singleName(tableId: TableId, columnId: ColumnId, langtag: String, name: String): ColumnDisplayInfos

  def multipleNames(tableId: TableId, columnId: ColumnId, langNames: List[(String, String)]): ColumnDisplayInfos

  def singleDesc(tableId: TableId, columnId: ColumnId, langtag: String, desc: String): ColumnDisplayInfos

  def multipleDescs(tableId: TableId, columnId: ColumnId, langDescs: List[(String, String)]): ColumnDisplayInfos

  def singleNameAndDesc(
      tableId: TableId,
      columnId: ColumnId,
      langtag: String,
      name: String,
      desc: String
  ): ColumnDisplayInfos

  def multipleNameAndDesc(
      tableId: TableId,
      columnId: ColumnId,
      infos: List[(String, String, String)]
  ): ColumnDisplayInfos

}

class ColumnDisplayInfosTestDirect extends AbstractColumnDisplayInfosTest {

  override def emptyDisplayInfo(tableId: TableId, columnId: ColumnId): ColumnDisplayInfos =
    ColumnDisplayInfos(tableId, columnId, List())

  override def singleName(tableId: TableId, columnId: ColumnId, langtag: String, name: String): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, List(NameOnly(langtag, name)))
  }

  override def multipleNames(
      tableId: TableId,
      columnId: ColumnId,
      langNames: List[(String, String)]
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, langNames.map(t => NameOnly(t._1, t._2)))
  }

  override def singleDesc(tableId: TableId, columnId: ColumnId, langtag: String, desc: String): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, List(DescriptionOnly(langtag, desc)))
  }

  override def multipleDescs(
      tableId: TableId,
      columnId: ColumnId,
      langDescs: List[(String, String)]
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, langDescs.map(t => DescriptionOnly(t._1, t._2)))
  }

  override def singleNameAndDesc(
      tableId: TableId,
      columnId: ColumnId,
      langtag: String,
      name: String,
      desc: String
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, List(NameAndDescription(langtag, name, desc)))
  }

  override def multipleNameAndDesc(
      tableId: TableId,
      columnId: ColumnId,
      infos: List[(String, String, String)]
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(
      tableId,
      columnId,
      infos.map {
        case (lang, name, null) => NameOnly(lang, name)
        case (lang, null, desc) => DescriptionOnly(lang, desc)
        case (lang, name, desc) => NameAndDescription(lang, name, desc)
      }
    )
  }

}

class ColumnDisplayInfosTestJsonObject extends AbstractColumnDisplayInfosTest {

  override def emptyDisplayInfo(tableId: TableId, columnId: ColumnId): ColumnDisplayInfos =
    ColumnDisplayInfos(tableId, columnId, DisplayInfos.fromJson(Json.obj()))

  override def singleName(tableId: TableId, columnId: ColumnId, langtag: String, name: String): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, DisplayInfos.fromJson(Json.obj("displayName" -> Json.obj(langtag -> name))))
  }

  override def multipleNames(
      tableId: TableId,
      columnId: ColumnId,
      langNames: List[(String, String)]
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(
      tableId,
      columnId,
      DisplayInfos.fromJson(Json.obj("displayName" -> langNames.foldLeft(Json.obj()) {
        case (json, (langtag, name)) => json.mergeIn(Json.obj(langtag -> name))
      }))
    )
  }

  override def singleDesc(tableId: TableId, columnId: ColumnId, langtag: String, desc: String): ColumnDisplayInfos = {
    ColumnDisplayInfos(tableId, columnId, DisplayInfos.fromJson(Json.obj("description" -> Json.obj(langtag -> desc))))
  }

  override def multipleDescs(
      tableId: TableId,
      columnId: ColumnId,
      langDescs: List[(String, String)]
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(
      tableId,
      columnId,
      DisplayInfos.fromJson(Json.obj("description" -> langDescs.foldLeft(Json.obj()) {
        case (json, (langtag, desc)) => json.mergeIn(Json.obj(langtag -> desc))
      }))
    )
  }

  override def singleNameAndDesc(
      tableId: TableId,
      columnId: ColumnId,
      langtag: String,
      name: String,
      desc: String
  ): ColumnDisplayInfos = {
    ColumnDisplayInfos(
      tableId,
      columnId,
      DisplayInfos.fromJson(
        Json.obj(
          "displayName" -> Json.obj(langtag -> name),
          "description" -> Json.obj(langtag -> desc)
        )
      )
    )
  }

  override def multipleNameAndDesc(
      tableId: TableId,
      columnId: ColumnId,
      infos: List[(String, String, String)]
  ): ColumnDisplayInfos = {
    val result = infos.foldLeft(Json.obj()) {
      case (json, (lang, name, null)) =>
        json.mergeIn(
          Json.obj("displayName" -> json.getJsonObject("displayName", Json.obj()).mergeIn(Json.obj(lang -> name)))
        )
      case (json, (lang, null, desc)) =>
        json.mergeIn(
          Json.obj("description" -> json.getJsonObject("description", Json.obj()).mergeIn(Json.obj(lang -> desc)))
        )
      case (json, (lang, name, desc)) =>
        val n = json.getJsonObject("displayName", Json.obj()).mergeIn(Json.obj(lang -> name))
        val d = json.getJsonObject("description", Json.obj()).mergeIn(Json.obj(lang -> desc))
        json.mergeIn(Json.obj("displayName" -> n)).mergeIn(Json.obj("description" -> d))
    }
    ColumnDisplayInfos(tableId, columnId, DisplayInfos.fromJson(result))
  }

}

class DisplayInfosTest {

  @Test
  def displayInfoFromString(): Unit = {
    Try(DisplayInfos.fromString("de", None.orNull, None.orNull)) match {
      case Failure(ex) => assertTrue(ex.isInstanceOf[IllegalArgumentException])
      case Success(_) => fail("Should throw an IllegalArgumentException")
    }

    assertEquals(NameOnly("de", "hallo"), DisplayInfos.fromString("de", "hallo", None.orNull))
    assertEquals(DescriptionOnly("de", "hallo"), DisplayInfos.fromString("de", None.orNull, "hallo"))
    assertEquals(NameAndDescription("de", "hallo", "hello"), DisplayInfos.fromString("de", "hallo", "hello"))
  }

  @Test
  def displayInfosFromJson(): Unit = {
    val json = Json.fromObjectString("""
                                       |{
                                       |  "displayName": {
                                       |    "en": "displayName en",
                                       |    "fr": "displayName fr"
                                       |  },
                                       |  "description" : {
                                       |    "en": "description en",
                                       |    "ch": "description ch"
                                       |  }
                                       |}
      """.stripMargin)

    val expected = List(
      NameAndDescription("en", "displayName en", "description en"),
      NameOnly("fr", "displayName fr"),
      DescriptionOnly("ch", "description ch")
    )

    assertEquals(expected, DisplayInfos.fromJson(json))
  }
}
