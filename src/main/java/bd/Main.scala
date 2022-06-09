package bd

import org.apache.spark.sql.functions.{col, mean, round, variance}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  // step 1
  val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val csv = sc.textFile("mtcars.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first
  val mtcdata = headerAndRows.filter(_(0) != header(0))

  // step 2
  val populationDF = mtcdata
    .map(p =>  (p(2), p(1)))
    .toDF("cyl", "mpg")

  // step 3
  populationDF
    .groupBy(col("cyl").as("Category"))
    .agg(round(mean("mpg"), 2).as("Mean"),
      round(variance("mpg"), 2).as("Var"))
    .sort("Category")
    .show()

  // step 4
  val sampleDF = populationDF.sample(false, 0.25)
  sampleDF.show()

  // step 5
  val resampleData = sampleDF.sample
}
