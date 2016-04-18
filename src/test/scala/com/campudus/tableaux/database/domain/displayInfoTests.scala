package com.campudus.tableaux.database.domain

import com.campudus.tableaux.database.model.TableauxModel.{ColumnId, TableId}
import org.junit.Assert._
import org.junit.Test
import org.vertx.scala.core.json.Json

abstract class AbstractDisplayInfosTest {

  @Test
  def checkSingleName(): Unit = {
    val di = singleName(1, 1, "de_DE", "Spalte 1")
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", "Spalte 1", null), di.binds)
  }

  @Test
  def checkMultipleNames(): Unit = {
    val di = multipleNames(1, 1, List("de_DE" -> "Spalte 1", "en_US" -> "Column 1"))
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", "Spalte 1", null, 1, 1, "en_US", "Column 1", null), di.binds)
  }

  @Test
  def checkSingleDescription(): Unit = {
    val di = singleDesc(1, 1, "de_DE", "Spalte 1 Beschreibung")
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", null, "Spalte 1 Beschreibung"), di.binds)
  }

  @Test
  def checkMultipleDescriptions(): Unit = {
    val di = multipleDescs(1, 1, List("de_DE" -> "Spalte 1 Beschreibung", "en_US" -> "Column 1 Description"))
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", null, "Spalte 1 Beschreibung", 1, 1, "en_US", null, "Column 1 Description"), di.binds)
  }

  @Test
  def checkSingleNameAndDescription(): Unit = {
    val di = singleNameAndDesc(1, 1, "de_DE", "Spalte 1", "Spalte 1 Beschreibung")
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", "Spalte 1", "Spalte 1 Beschreibung"), di.binds)
  }

  @Test
  def checkMultipleNamesAndDescriptions(): Unit = {
    val di = multipleNameAndDesc(1, 1, List(("de_DE", "Spalte 1", "Spalte 1 Beschreibung"),
      ("en_US", "Column 1", "Column 1 Description")))
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", "Spalte 1", "Spalte 1 Beschreibung", 1, 1, "en_US", "Column 1", "Column 1 Description"), di.binds)
  }

  @Test
  def checkNameAndOtherDesc(): Unit = {
    val di = multipleNameAndDesc(1, 1, List(("de_DE", "Spalte 1", null), ("en_US", null, "Column 1 Description")))
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    assertEquals(Seq(1, 1, "de_DE", "Spalte 1", null, 1, 1, "en_US", null, "Column 1 Description"), di.binds)
  }

  @Test
  def checkCombinations(): Unit = {
    val di = multipleNameAndDesc(1, 1, List(
      ("de_DE", "Spalte 1", "Spalte 1 Beschreibung"),
      ("en_US", null, "Column 1 Description"),
      ("fr_FR", "Colonne 1", null)
    ))
    assertTrue(di.nonEmpty)
    assertEquals(
      s"""INSERT INTO system_columns_lang (table_id, column_id, langtag, name, description)
          |VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)""".stripMargin, di.statement)
    val all = Seq(
      Seq(1, 1, "de_DE", "Spalte 1", "Spalte 1 Beschreibung"),
      Seq(1, 1, "en_US", null, "Column 1 Description"),
      Seq(1, 1, "fr_FR", "Colonne 1", null)
    )

    assertTrue(all.forall(s => di.binds.indexOfSlice(s) >= 0))
  }

  @Test
  def emptyDisplayStuff(): Unit = {
    val di = emptyDisplayInfo(1, 1)
    assertFalse(di.nonEmpty)
  }

  def emptyDisplayInfo(tableId: TableId, columnId: ColumnId): DisplayInfos

  def singleName(tableId: TableId, columnId: ColumnId, langtag: String, name: String): DisplayInfos

  def multipleNames(tableId: TableId, columnId: ColumnId, langNames: List[(String, String)]): DisplayInfos

  def singleDesc(tableId: TableId, columnId: ColumnId, langtag: String, desc: String): DisplayInfos

  def multipleDescs(tableId: TableId, columnId: ColumnId, langDescs: List[(String, String)]): DisplayInfos

  def singleNameAndDesc(tableId: TableId, columnId: ColumnId, langtag: String, name: String, desc: String): DisplayInfos

  def multipleNameAndDesc(tableId: TableId, columnId: ColumnId, infos: List[(String, String, String)]): DisplayInfos

}

class DisplayInfoTestDirect extends AbstractDisplayInfosTest {

  override def emptyDisplayInfo(tableId: TableId, columnId: ColumnId): DisplayInfos =
    DisplayInfos(tableId, columnId, List())

  override def singleName(tableId: TableId, columnId: ColumnId, langtag: String, name: String): DisplayInfos =
    DisplayInfos(tableId, columnId, List(NameOnly(langtag, name)))

  override def multipleNames(tableId: TableId, columnId: ColumnId, langNames: List[(String, String)]): DisplayInfos =
    DisplayInfos(tableId, columnId, langNames.map(t => NameOnly(t._1, t._2)))

  override def singleDesc(tableId: TableId, columnId: ColumnId, langtag: String, desc: String): DisplayInfos =
    DisplayInfos(tableId, columnId, List(DescriptionOnly(langtag, desc)))

  override def multipleDescs(tableId: TableId, columnId: ColumnId, langDescs: List[(String, String)]): DisplayInfos =
    DisplayInfos(tableId, columnId, langDescs.map(t => DescriptionOnly(t._1, t._2)))

  override def singleNameAndDesc(tableId: TableId, columnId: ColumnId, langtag: String, name: String, desc: String): DisplayInfos =
    DisplayInfos(tableId, columnId, List(NameAndDescription(langtag, name, desc)))

  override def multipleNameAndDesc(tableId: TableId, columnId: ColumnId, infos: List[(String, String, String)]): DisplayInfos =
    DisplayInfos(tableId, columnId, infos.map {
      case (lang, name, null) => NameOnly(lang, name)
      case (lang, null, desc) => DescriptionOnly(lang, desc)
      case (lang, name, desc) => NameAndDescription(lang, name, desc)
    })

}

class DisplayInfoTestJsonObject extends AbstractDisplayInfosTest {

  override def emptyDisplayInfo(tableId: TableId, columnId: ColumnId): DisplayInfos =
    DisplayInfos(tableId, columnId, Json.obj())

  override def singleName(tableId: TableId, columnId: ColumnId, langtag: String, name: String): DisplayInfos =
    DisplayInfos(tableId, columnId, Json.obj("displayName" -> Json.obj(langtag -> name)))

  override def multipleNames(tableId: TableId, columnId: ColumnId, langNames: List[(String, String)]): DisplayInfos = {
    DisplayInfos(tableId, columnId, Json.obj("displayName" -> langNames.foldLeft(Json.obj()) {
      case (json, (langtag, name)) => json.mergeIn(Json.obj(langtag -> name))
    }))
  }

  override def singleDesc(tableId: TableId, columnId: ColumnId, langtag: String, desc: String): DisplayInfos =
    DisplayInfos(tableId, columnId, Json.obj("description" -> Json.obj(langtag -> desc)))

  override def multipleDescs(tableId: TableId, columnId: ColumnId, langDescs: List[(String, String)]): DisplayInfos =
    DisplayInfos(tableId, columnId, Json.obj("description" -> langDescs.foldLeft(Json.obj()) {
      case (json, (langtag, desc)) => json.mergeIn(Json.obj(langtag -> desc))
    }))

  override def singleNameAndDesc(tableId: TableId, columnId: ColumnId, langtag: String, name: String, desc: String): DisplayInfos =
    DisplayInfos(tableId, columnId, Json.obj(
      "displayName" -> Json.obj(langtag -> name),
      "description" -> Json.obj(langtag -> desc)
    ))

  override def multipleNameAndDesc(tableId: TableId, columnId: ColumnId, infos: List[(String, String, String)]): DisplayInfos = {
    val result = infos.foldLeft(Json.obj()) {
      case (json, (lang, name, null)) => json.mergeIn(Json.obj("displayName" -> json.getJsonObject("displayName", Json.obj()).mergeIn(Json.obj(lang -> name))))
      case (json, (lang, null, desc)) => json.mergeIn(Json.obj("description" -> json.getJsonObject("description", Json.obj()).mergeIn(Json.obj(lang -> desc))))
      case (json, (lang, name, desc)) =>
        val n = json.getJsonObject("displayName", Json.obj()).mergeIn(Json.obj(lang -> name))
        val d = json.getJsonObject("description", Json.obj()).mergeIn(Json.obj(lang -> desc))
        json.mergeIn(Json.obj("displayName" -> n)).mergeIn(Json.obj("description" -> d))
    }
    DisplayInfos(tableId, columnId, result)
  }

}