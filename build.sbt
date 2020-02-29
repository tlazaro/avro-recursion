lazy val root = Project("avro", file(".")) settings (Seq[Setting[_]](
  scalaVersion := "2.12.10",
  organization := "com.belfrygames",
  version := "0.1",
  libraryDependencies ++= Seq(
    "io.higherkindness"    %% "droste-core"     % "0.8.0",
    "io.higherkindness"    %% "droste-macros"   % "0.8.0",
    "com.slamdata"         %% "matryoshka-core" % "0.21.3",
    "org.apache.avro"      % "avro"             % "1.9.1",
    "org.slf4j"            % "slf4j-log4j12"    % "1.7.25",
    "com.beachape"         %% "enumeratum"      % "1.5.13",
    "org.spire-math"       %% "spire"           % "0.13.0",
    "org.typelevel"        %% "cats-core"       % "1.3.1",
    "com.github.pathikrit" %% "better-files"    % "3.8.0",
    "io.circe"             %% "circe-core"      % "0.11.1",
    "io.circe"             %% "circe-generic"   % "0.11.1",
    "io.circe"             %% "circe-parser"    % "0.11.1",
    "org.scalactic"        %% "scalactic"       % "3.0.4",
    "org.scalatest"        %% "scalatest"       % "3.0.4" % "test",
    "org.scalacheck"       %% "scalacheck"      % "1.13.4" % "test"
  ),
  javacOptions ++= Seq("-encoding", "UTF-8"),
  scalacOptions += "-Ypartial-unification",
  fork in Test := false,
  fork := true,
  addCompilerPlugin("org.typelevel"   %% "kind-projector" % "0.11.0" cross CrossVersion.full),
  addCompilerPlugin("org.scalamacros" % "paradise"        % "2.1.1" cross CrossVersion.full)
): _*)
