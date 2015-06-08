import _root_.io.vertx.scala.build.{VertxModule, VertxProject}

import sbt._
import sbt.Keys._

object TableauxBuild extends VertxProject {
  val module = VertxModule("com.campudus", "tableaux", "0.1.0", "tables into sheets", Some("2.11.6"), Seq("2.11.6"))

  override lazy val customSettings = Seq(
    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % "2.8",

      "io.vertx" %% "mod-mysql-postgresql" % "0.3.1" % "provided",

      "io.vertx" % "testtools" % "2.0.3-final" % "test",
      "org.hamcrest" % "hamcrest-library" % "1.3" % "test",
      "com.novocode" % "junit-interface" % "0.10" % "test"
    )
  )
}