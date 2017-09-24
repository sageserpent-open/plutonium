import sbt.Keys.libraryDependencies

lazy val settings = Seq(
  organization := "com.sageserpent",
  name := "open-plutonium-test-against-release",
  scalaVersion := "2.12.1",
  scalacOptions += "-Xexperimental",
  libraryDependencies += "com.sageserpent"   %% "open-plutonium"              % "1.1.0",
  libraryDependencies += "org.slf4j"         % "slf4j-api"                    % "1.7.21" % "provided",
  libraryDependencies += "org.slf4j"         % "slf4j-nop"                    % "1.7.21" % "test",
  libraryDependencies += "org.scalatest"     %% "scalatest"                   % "3.0.1" % "test",
  libraryDependencies += "org.scalacheck"    %% "scalacheck"                  % "1.13.5" % "test",
  libraryDependencies += "org.scalamock"     %% "scalamock-scalatest-support" % "3.4.2" % "test",
  libraryDependencies += "org.scalaz"        %% "scalaz-scalacheck-binding"   % "7.3.0-M10" % "test",
  libraryDependencies += "com.github.kstyrc" % "embedded-redis"               % "0.6" % "test",
  libraryDependencies += "junit"             % "junit"                        % "4.12" % "test",
  libraryDependencies += "com.novocode"      % "junit-interface"              % "0.11" % "test"
)

lazy val plutonium = (project in file(".")).settings(settings: _*)

resolvers += Resolver.jcenterRepo
