package bd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean, round, sum, variance}
import org.apache.spark.sql
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.math.BigDecimal.RoundingMode
import scala.math._

object Main extends App {

  if (args.length != 2) {
    println("Application arguments must be 2 as: <sampling percentage> <num of iteration>")
    sys.exit()
  }

  // step 1
  val spark = SparkSession.builder()
    .appName("Spark and SparkSQL")
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  val csv = sc.textFile("input/mtcars.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first
  val mtcdata = headerAndRows.filter(_ (0) != header(0))

  // step 2
  val populationDF = mtcdata
    .map(p => (p(2), p(1)))
    .toDF("cyl", "mpg")

  // step 3
  println("Actual")

  populationDF
    .groupBy(col("cyl").as("Category"))
    .agg(round(mean("mpg"), 2).as("Mean"),
      round(variance("mpg"), 2).as("Var"))
    .sort("Category")
    .show()

  // step 4, 5 & 6
  val schema = StructType(Array(
    StructField("cyl", StringType, nullable = false),
    StructField("mpg", StringType, nullable = false)
  ))

  val samplingPercentage = args(0).toInt
  val iteration = args(1).toInt
  println(s"Estimate: ${samplingPercentage}, Iteration: ${iteration}")

  val agg4 = sampleAndAgg(populationDF, 4, samplingPercentage, iteration)
  val agg6 = sampleAndAgg(populationDF, 6, samplingPercentage, iteration)
  val agg8 = sampleAndAgg(populationDF, 8, samplingPercentage, iteration)

  Seq((4, agg4._1, agg4._2), (6, agg6._1, agg6._2), (8, agg8._1, agg8._2))
    .toDF("Category", "Mean", "Var")
    .show()

  def sampleAndAgg(df: sql.DataFrame, cyl: Int, samplingPercentage: Int, iteration: Int): (Double, Double) = {
    val sample25 = df
      .filter($"cyl" === cyl).rdd
      .takeSample(withReplacement = false, (df.count()*samplingPercentage/100).toInt)
    val sample25RDD = sc.makeRDD(sample25)

    var meanSum, varSum = BigDecimal(0)
    for (_ <- 1 to iteration) {
      val sample100 = sample25RDD
        .takeSample(withReplacement = true, sample25.length)

      val agg = spark.createDataFrame(sc.parallelize(sample100), schema)
        .agg(mean("mpg"),
          variance("mpg"))
        .collect()

      meanSum += agg(0).getDouble(0)
      varSum += agg(0).getDouble(1)
    }

    ((meanSum / iteration).setScale(2, RoundingMode.HALF_UP).toDouble,
      (varSum / iteration).setScale(2, RoundingMode.HALF_UP).toDouble)
  }

//  def myMean(arr : Array[sql.Row]): (Double, Double) = {
//    val arr0 = arr.map(_.getDouble(0))
//    val arr1 = arr.map(_.getDouble(1))
//    (arr0.sum / arr0.length, arr1.sum / arr1.length)
//  }
}
