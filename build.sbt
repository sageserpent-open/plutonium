name := "Plutonium"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.scala-lang.modules" %% "spores-core" % "0.1.3"

libraryDependencies += "org.scala-lang.modules" %% "spores-pickling" % "0.1.3"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.4" % "test"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.3"

libraryDependencies += "org.scalaz" %% "scalaz-scalacheck-binding" % "7.1.3" % "test"

libraryDependencies += "org.spire-math" %% "spire" % "0.10.1"

libraryDependencies += "io.github.nicolasstucki" %% "multisets" % "0.3"

publishMavenStyle := true
