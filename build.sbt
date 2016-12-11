import sbt.Keys.libraryDependencies

lazy val settings = Seq(organization := "com.sageserpent",
  name := "plutonium",
  version := "1.0.2-SNAPSHOT",
  scalaVersion := "2.11.8",
  scalacOptions += "-Xexperimental",

  libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.8",
  libraryDependencies += "cglib" % "cglib" % "3.2.4",
  libraryDependencies += "net.bytebuddy" % "byte-buddy" % "1.5.8",
  libraryDependencies += "com.sageserpent" %% "americium" % "0.1.1",
  libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8",
  libraryDependencies += "biz.paluch.redis" % "lettuce" % "4.3.0.Final",
  libraryDependencies += "io.reactivex" % "rxscala_2.11" % "0.26.4",
  libraryDependencies += "io.github.nicolasstucki" %% "multisets" % "0.3",
  libraryDependencies += "com.twitter" % "chill_2.11" % "0.8.1",

  libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.21" % "provided",
  libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.21" % "test",

  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.4.2" % "test",
  libraryDependencies += "org.scalaz" % "scalaz-scalacheck-binding_2.11" % "7.2.8-scalacheck-1.13" % "test",
  libraryDependencies += "com.github.kstyrc" % "embedded-redis" % "0.6" % "test",
  libraryDependencies += "junit" % "junit" % "4.12" % "test",
  libraryDependencies += "com.google.guava" % "guava" % "20.0",

  publishMavenStyle := true,
  publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))))


lazy val plutonium = (project in file(".")).settings(settings: _*)

resolvers += Resolver.jcenterRepo