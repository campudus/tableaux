package com.campudus.tableaux.helper

import java.io.File

/**
  * Minimal drop-in replacement for `scala.reflect.io.Path`, which shipped in the `scala-reflect` jar and has no Scala 3
  * equivalent. Only the operations actually used by call sites in this codebase are provided.
  */
case class Path(path: String) {

  def jfile: File = new File(path)

  def /(child: Path): Path = Path(new File(jfile, child.path).getPath)

  def /(child: String): Path = Path(new File(jfile, child).getPath)

  def extension: String = {
    val name = jfile.getName
    val dot = name.lastIndexOf('.')
    if (dot >= 0 && dot < name.length - 1) name.substring(dot + 1) else ""
  }

  def name: String = jfile.getName

  def parent: Path = Path(Option(jfile.getParent).getOrElse(""))

  def exists: Boolean = jfile.exists()

  override def toString: String = path
}

object Path {
  def apply(file: File): Path = Path(file.getPath)

  given Conversion[String, Path] = (s: String) => Path(s)
}
