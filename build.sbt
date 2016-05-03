lazy val settings = Seq(organization := "com.sageserpent",
  name := "plutonium",
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.11.7",
  scalacOptions += "-Xexperimental",
  libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.4" % "test",
  libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.2",
  libraryDependencies += "org.scalaz" %% "scalaz-scalacheck-binding" % "7.1.3" % "test",
  libraryDependencies += "io.github.nicolasstucki" %% "multisets" % "0.3",
  libraryDependencies += "cglib" % "cglib" % "3.2.0",
  libraryDependencies += "com.sageserpent" %% "americium" % "0.1.0",
  libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.4",
  libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test",
  publishMavenStyle := true,
  publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))))


lazy val plutonium = (project in file(".")).settings(settings: _*)

resolvers += Resolver.jcenterRepo