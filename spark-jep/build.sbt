import com.typesafe.sbt.SbtNativePackager.Docker
import com.typesafe.sbt.packager.Keys.{daemonUser, daemonUserUid, dockerCommands, dockerPackageMappings}
import com.typesafe.sbt.packager.MappingsHelper.directory
import com.typesafe.sbt.packager.docker.ExecCmd
import sbt.Keys.javaOptions

scalaVersion in ThisBuild := "2.12.10"

val sparkVersion = "3.1.2"


val pythonLibs = Seq(
  "black.ninia" % "jep" % "3.9.1"
)
val sparkLibs = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% s"spark-kubernetes" % sparkVersion
)

def nativeLibraryPath = s"${sys.env.get("JAVA_LIBRARY_PATH") orElse sys.env.get("LD_LIBRARY_PATH") orElse sys.env.get("DYLD_LIBRARY_PATH") getOrElse "."}:."

// JAR build settings
lazy val commonSettings = Seq(
  organization := "imec",
  version := "0.1",
  scalaSource in Compile := baseDirectory.value / "src" / "main"/ "scala",
  //scalaSource in Test := baseDirectory.value / "test",
  resourceDirectory in Test := baseDirectory.value / "test" / "resources",
  //javacOptions ++= Seq(s"-Djava.library.path=$nativeLibraryPath"),
  javaOptions  ++= Seq(s"-Djava.library.path=$nativeLibraryPath"),
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-language:implicitConversions",
    "-language:postfixOps"
  ),
  libraryDependencies ++= sparkLibs ++ pythonLibs
)

// Docker Image build settings
dockerBaseImage := "acrsparksdp.azurecr.io/sharedruntime:test"

//val registry = "192.168.64.11:5000"

lazy val root = (project in file("."))
  .enablePlugins(
    DockerPlugin,
    JavaAppPackaging
  )
  .settings(
    Docker / packageName := s"acrsparksdp.azurecr.io/spark",
    name := "spark-spacy-example",
    commonSettings,
    mainClass in (Compile, run) := Some("dzlab.ScalaSpacyExample")
  )