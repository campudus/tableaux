resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0")

libraryDependencies +=  "io.vertx" % "vertx-platform" % "2.1.5"