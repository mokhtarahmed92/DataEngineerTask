name := "HelloWorld1"

version := "0.1"

scalaVersion := "2.10.6"



libraryDependencies ++= {
  val sparkVer = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer ,
    "org.apache.spark" %% "spark-sql" % sparkVer
  )
}
