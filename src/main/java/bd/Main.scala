package bd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean, round, sum, variance}
import org.apache.spark.sql
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.math.BigDecimal.RoundingMode
import scala.math._

object Main extends App {
  // step 1
  val spark = SparkSession.builder()
    .appName("Spark and SparkSQL")
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  val csv = sc.textFile("mtcars.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first
  val mtcdata = headerAndRows.filter(_ (0) != header(0))

  // step 2
  val populationDF = mtcdata
    .map(p => (p(2), p(1)))
    .toDF("cyl", "mpg")

  // step 3
  populationDF
    .groupBy(col("cyl").as("Category"))
    .agg(round(mean("mpg"), 2).as("Mean"),
      round(variance("mpg"), 2).as("Var"))
    .sort("Category")
    .show()

  // step 4 & step 5
  val schema = StructType(Array(
    StructField("cyl", StringType, nullable = false),
    StructField("mpg", StringType, nullable = false)
  ))
  val agg4 = sampleAndAgg(populationDF, 4)
  val agg6 = sampleAndAgg(populationDF, 6)
  val agg8 = sampleAndAgg(populationDF, 8)
  Seq((4, agg4._1, agg4._2), (6, agg6._1, agg6._2), (8, agg8._1, agg8._2))
    .toDF("Category", "Mean", "Var")
    .show()

  def sampleAndAgg(df: sql.DataFrame, cyl: Int): (BigDecimal, BigDecimal) = {
    val sample25 = df
      .filter($"cyl" === cyl).rdd
      .takeSample(withReplacement = false, (df.count()*25/100).toInt)
    val sample25RDD = sc.makeRDD(sample25)

    var meanSum, varSum = BigDecimal(0)
    for (_ <- 1 to 1000) {
      val sample100 = sample25RDD
        .takeSample(withReplacement = true, sample25.length)

      spark.createDataFrame(sc.parallelize(sample100), schema)
        .groupBy(col("cyl"))
        .agg(mean("mpg"),
          variance("mpg"))
        .foreach(r => {
          meanSum += r.getDouble(1)
          varSum += r.getDouble(2)
        })
    }

    ((meanSum / 1000).setScale(2, RoundingMode.HALF_UP),
      (varSum / 1000).setScale(2, RoundingMode.HALF_UP))
  }
}
