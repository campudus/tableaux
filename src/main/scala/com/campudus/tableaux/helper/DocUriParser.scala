package com.campudus.tableaux.helper

object DocUriParser {

  type SchemeHostAndPath = (String, String, String)

  val DEFAULT_VALUES: SchemeHostAndPath = ("http", "localhost:8181", "")

  def parse(absoluteUri: String): SchemeHostAndPath = {
    val UriMatcher = "(https?)://([^/]+)/?(.*)/docs.*".r
    absoluteUri match {
      case UriMatcher(scheme, host, path) => (scheme, host, path)
      case _ => DEFAULT_VALUES
    }
  }

}
