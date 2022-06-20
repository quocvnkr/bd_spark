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
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  val csv = sc.textFile("input/mtcars.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first
  val mtcData = headerAndRows.filter(_ (0) != header(0))

  // step 2
  val populationDF = mtcData
    .map(p => (p(2), p(1)))
    .toDF("cyl", "mpg")

  println("==================== STEP 3 ====================")
  println()
  println("Actual")

  populationDF
    .groupBy(col("cyl").as("Category"))
    .agg(round(mean("mpg"), 2).as("Mean"),
      round(variance("mpg"), 2).as("Var"))
    .sort("Category")
    .show()

  /*
  populationDF.createOrReplaceTempView("population")

  sqlContext.sql("" +
    "SELECT cyl as Category, round(mean(mpg),2) as Mean, round(variance(mpg),2) as Var " +
    "FROM population " +
    "GROUP BY cyl " +
    "ORDER BY cyl ").show()
   */

  println("================= STEP 4, 5, 6 =================")
  println()

  val samplingPercentage = args(0).toInt
  val iteration = args(1).toInt
  println(s"Sampling percentage (%): ${samplingPercentage}, Iteration: ${iteration}")

  val schema = StructType(Array(
    StructField("cyl", StringType, nullable = false),
    StructField("mpg", StringType, nullable = false)
  ))

  val categories = mtcData.map(p => p(2).toInt).distinct().collect()
  var seqAggs = Seq[(Int, Double, Double)]()
  categories.foreach(x => {
    val samplingResult = sampleAndAgg(populationDF, x, samplingPercentage, iteration)
    seqAggs = seqAggs :+ (x, samplingResult._1, samplingResult._2)
  })
  println("Estimation Result")
  seqAggs.toDF("Rank", "Mean", "Var").show

  def sampleAndAgg(df: sql.DataFrame, cyl: Int, samplingPercentage: Int, iteration: Int): (Double, Double) = {
    val sample = df
      .filter($"cyl" === cyl).rdd
      .takeSample(withReplacement = false, (df.count()*samplingPercentage/100).toInt)
    val sampleRDD = sc.makeRDD(sample)

    var meanSum, varSum = BigDecimal(0)
    for (_ <- 1 to iteration) {
      val resample = sampleRDD.takeSample(withReplacement = true, sample.length)
      val agg = spark.createDataFrame(sc.parallelize(resample), schema)
        .agg(mean("mpg"),
          variance("mpg"))
        .collect()

      meanSum += agg(0).getDouble(0)
      varSum += agg(0).getDouble(1)

//      meanSum += myMean(resample)
//      varSum += myVariance(resample)
    }

    ((meanSum / iteration).setScale(2, RoundingMode.HALF_UP).toDouble,
      (varSum / iteration).setScale(2, RoundingMode.HALF_UP).toDouble)
  }

  /*

  def myMean(arr : Array[sql.Row]): Double = {
    val arr1 = arr.map(_.getString(1).toDouble)
    arr1.sum / arr1.length
  }

  def myVariance(arr : Array[sql.Row]): Double = {
    val avg = myMean(arr)
    arr.map(_.getString(1).toDouble).map(a => math.pow(a - avg, 2)).sum / arr.length
  }

  */
}
