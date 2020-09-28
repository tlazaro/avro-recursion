lazy val root = Project("avro", file(".")) settings (Seq[Setting[_]](
  scalaVersion := "2.13.3",
  organization := "com.belfrygames",
  version := "0.1",
  libraryDependencies ++= Seq(
    "io.higherkindness"    %% "droste-core"   % "0.8.0",
    "io.higherkindness"    %% "droste-macros" % "0.8.0",
    "org.apache.avro"      % "avro"           % "1.9.2",
    "org.slf4j"            % "slf4j-log4j12"  % "1.7.30",
    "com.beachape"         %% "enumeratum"    % "1.6.1",
    "org.typelevel"        %% "cats-core"     % "2.2.0",
    "org.typelevel"        %% "kittens"       % "2.1.0",
    "com.github.pathikrit" %% "better-files"  % "3.9.1",
    "io.circe"             %% "circe-core"    % "0.13.0",
    "io.circe"             %% "circe-generic" % "0.13.0",
    "io.circe"             %% "circe-parser"  % "0.13.0",
    "org.scalactic"        %% "scalactic"     % "3.2.2",
    "org.scalatest"        %% "scalatest"     % "3.2.2" % "test",
    "org.scalacheck"       %% "scalacheck"    % "1.14.3" % "test"
  ),
  javacOptions ++= Seq("-encoding", "UTF-8"),
  fork in Test := false,
  fork := true,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
  // scalacOptions += "-Ypartial-unification",
  // addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
): _*)
