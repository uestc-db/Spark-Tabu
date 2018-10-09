name:="Logistics project"
version:="1.0"
scalaVersion:="2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.3"
libraryDependencies += "com.opencsv" % "opencsv" % "4.0"

unmanagedJars in Compile ++= Seq(
    Attributed.blank[File](file(baseDirectory.value + "./lib/archery_32.jar"))
  )
