import sbt.Configurations.config
import sbt.Defaults.testSettings
import sbt.Keys.libraryDependencies

lazy val settings = Seq(
  organization := "com.sageserpent",
  name := "open-plutonium",
  scalaVersion := "2.12.1",
  scalacOptions += "-Xexperimental",
  libraryDependencies += "org.scalaz"              %% "scalaz-core"                 % "7.3.0-M10",
  libraryDependencies += "net.bytebuddy"           % "byte-buddy"                   % "1.7.6",
  libraryDependencies += "com.sageserpent"         %% "americium"                   % "0.1.4",
  libraryDependencies += "com.jsuereth"            %% "scala-arm"                   % "2.0",
  libraryDependencies += "org.scala-lang"          % "scala-reflect"                % "2.12.1",
  libraryDependencies += "biz.paluch.redis"        % "lettuce"                      % "4.3.0.Final",
  libraryDependencies += "io.reactivex"            %% "rxscala"                     % "0.26.4",
  libraryDependencies += "io.github.nicolasstucki" %% "multisets"                   % "0.4",
  libraryDependencies += "com.twitter"             %% "chill"                       % "0.9.2",
  libraryDependencies += "org.slf4j"               % "slf4j-api"                    % "1.7.21" % "provided",
  libraryDependencies += "org.slf4j"               % "slf4j-nop"                    % "1.7.21" % "test",
  libraryDependencies += "org.scalatest"           %% "scalatest"                   % "3.0.1" % "test",
  libraryDependencies += "org.scalacheck"          %% "scalacheck"                  % "1.13.5" % "test",
  libraryDependencies += "org.scalamock"           %% "scalamock-scalatest-support" % "3.4.2" % "test",
  libraryDependencies += "org.scalaz"              %% "scalaz-scalacheck-binding"   % "7.3.0-M10" % "test",
  libraryDependencies += "com.github.kstyrc"       % "embedded-redis"               % "0.6" % "test",
  libraryDependencies += "junit"                   % "junit"                        % "4.12" % "test",
  libraryDependencies += "com.novocode"            % "junit-interface"              % "0.11" % "test",
  libraryDependencies += "com.storm-enroute"       %% "scalameter"                  % "0.8.2" % "benchmark",
  testFrameworks in Benchmark += new TestFramework(
    "org.scalameter.ScalaMeterFramework"),
  publishMavenStyle := true,
  bintrayReleaseOnPublish in ThisBuild := false,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  bintrayVcsUrl := Some("git@github.com:sageserpent-open/open-plutonium.git")
)

lazy val Benchmark = config("benchmark") extend Test

lazy val plutonium = (project in file("."))
  .configs(Benchmark)
  .settings(settings ++ inConfig(Benchmark)(testSettings): _*)

resolvers += Resolver.jcenterRepo

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"
