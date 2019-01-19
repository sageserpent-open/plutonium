import sbt.Configurations.config
import sbt.Defaults.testSettings
import sbt.Keys.libraryDependencies

lazy val settings = Seq(
  organization := "com.sageserpent",
  name := "plutonium",
  scalaVersion := "2.12.4",
  scalacOptions ++= Seq("-Xexperimental", "-target:jvm-1.8"),
  libraryDependencies += "org.scalaz"              %% "scalaz-core"                  % "7.3.0-M10",
  libraryDependencies += "net.bytebuddy"           % "byte-buddy"                    % "1.9.7",
  libraryDependencies += "com.sageserpent"         %% "americium"                    % "0.1.5",
  libraryDependencies += "com.jsuereth"            %% "scala-arm"                    % "2.0",
  libraryDependencies += "org.scala-lang"          % "scala-reflect"                 % "2.12.4",
  libraryDependencies += "io.lettuce"              % "lettuce-core"                  % "5.1.3.RELEASE",
  libraryDependencies += "org.scala-lang.modules"  %% "scala-java8-compat"           % "0.9.0",
  libraryDependencies += "io.netty"                % "netty-transport-native-epoll"  % "4.1.32.Final" classifier "linux-x86_64",
  libraryDependencies += "io.netty"                % "netty-transport-native-kqueue" % "4.1.32.Final" classifier "osx-x86_64",
  libraryDependencies += "io.projectreactor"       %% "reactor-scala-extensions"     % "0.3.5",
  libraryDependencies += "io.github.nicolasstucki" %% "multisets"                    % "0.4",
  libraryDependencies += "com.twitter"             %% "chill"                        % "0.9.3",
  libraryDependencies += "io.verizon.quiver"       %% "core"                         % "7.0.19",
  libraryDependencies += "de.ummels"               %% "scala-prioritymap"            % "1.0.0",
  libraryDependencies += "de.sciss"                %% "fingertree"                   % "1.5.2",
  libraryDependencies += "org.slf4j"               % "slf4j-api"                     % "1.7.21" % "provided",
  libraryDependencies += "org.slf4j"               % "slf4j-nop"                     % "1.7.21" % "test",
  libraryDependencies += "org.scalatest"           %% "scalatest"                    % "3.0.5" % "test",
  libraryDependencies += "org.scalacheck"          %% "scalacheck"                   % "1.13.5" % "test",
  libraryDependencies += "org.scalamock"           %% "scalamock"                    % "4.1.0" % "test",
  libraryDependencies += "org.scalaz"              %% "scalaz-scalacheck-binding"    % "7.3.0-M10" % "test",
  libraryDependencies += "it.ozimov"               % "embedded-redis"                % "0.7.2" % "test",
  libraryDependencies += "junit"                   % "junit"                         % "4.12" % "test",
  libraryDependencies += "com.novocode"            % "junit-interface"               % "0.11" % "test",
  libraryDependencies += "com.storm-enroute"       %% "scalameter"                   % "0.8.2" % "benchmark",
  testFrameworks in Benchmark += new TestFramework(
    "org.scalameter.ScalaMeterFramework"),
  publishMavenStyle := true,
  bintrayReleaseOnPublish in ThisBuild := false,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  bintrayVcsUrl := Some("git@github.com:sageserpent-open/plutonium.git"),
  parallelExecution in Test := false,
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / publishArtifact := false
)

lazy val Benchmark = config("benchmark") extend Test

lazy val plutonium = (project in file("."))
  .configs(Benchmark)
  .settings(settings ++ inConfig(Benchmark)(testSettings): _*)

resolvers += Resolver.jcenterRepo

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"
