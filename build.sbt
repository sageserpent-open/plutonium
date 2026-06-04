import sbt.Configurations.config
import sbt.Defaults.testSettings
import sbt.Keys.libraryDependencies

lazy val openSesames = Seq(
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

lazy val javaVersion = "17"

ThisBuild / scalaVersion := "2.13.18"

ThisBuild / javacOptions ++= Seq("-source", javaVersion, "-target", javaVersion)

ThisBuild / scalacOptions ++= Seq(s"--release:$javaVersion")

lazy val settings = Seq(
  organization := "com.sageserpent",
  name         := "plutonium",
  libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
  libraryDependencies += "org.typelevel" %% "cats-core"      % "2.13.0",
  libraryDependencies += "org.typelevel" %% "alleycats-core" % "2.13.0",
  libraryDependencies += "org.typelevel" %% "cats-effect"    % "3.7.0",
  libraryDependencies += "net.bytebuddy"  % "byte-buddy"     % "1.18.7",
  libraryDependencies += "org.scala-lang.modules" %% "scala-collection-contrib" % "0.4.0",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.13.18",
  libraryDependencies += "io.altoo" %% "scala-kryo-serialization" % "1.5.0",
  libraryDependencies += "io.getnelson.quiver" %% "core"       % "8.0.9",
  libraryDependencies += "de.sciss"            %% "fingertree" % "1.5.5",
  libraryDependencies += "com.google.guava"     % "guava"      % "33.5.0-jre",
  libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "3.2.3",
  libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "4.3.5",
  libraryDependencies += "com.h2database" % "h2" % "2.1.212", // Versions >= 2.1.214 break the tests for `BlobStorageOnH2`.
  libraryDependencies += "com.zaxxer" % "HikariCP"  % "7.0.2",
  libraryDependencies += "org.slf4j"  % "slf4j-api" % "2.0.17" % "provided",
  libraryDependencies += "com.sageserpent" %% "americium-junit5" % "2.1.2" % "test",
  libraryDependencies += "com.sageserpent" %% "americium-utilities" % "2.1.2" % "test",
  libraryDependencies += "com.eed3si9n.expecty" %% "expecty" % "0.17.1" % "test",
  libraryDependencies += "org.slf4j"       % "slf4j-nop"  % "2.0.17" % "test",
  libraryDependencies += "org.scalatest"  %% "scalatest"  % "3.2.20" % "test",
  libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.19.0" % "test",
  libraryDependencies += "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % "test",
  libraryDependencies += "org.scalamock" %% "scalamock" % "7.5.5"  % "test",
  libraryDependencies += "com.github.sbt.junit" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % "test",
  libraryDependencies += "org.typelevel" %% "cats-laws" % "2.13.0" % "test",
  libraryDependencies += "org.typelevel" %% "cats-testkit-scalatest" % "2.1.5" % "test",
  libraryDependencies += "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5" % "test",
  libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0" % "test",
  publishMavenStyle := true,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  bintrayVcsUrl := Some("git@github.com:sageserpent-open/plutonium.git"),
  Test / fork   := true,
  Test / testForkedParallel := true,
  Test / javaOptions ++= openSesames,
  Benchmark / fork := true,
  Benchmark / javaOptions ++= openSesames
)

lazy val Benchmark = config("benchmark") extend Test

lazy val plutonium = (project in file("."))
  .configs(Benchmark)
  .settings(settings ++ inConfig(Benchmark)(testSettings): _*)
