package bd

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean, regexp_replace, round, variance}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.math.BigDecimal.RoundingMode
import scala.math._

object MainSalary extends App {

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

  val csv = sc.textFile("input/Salaries.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim.replaceAll("\"", "")))
  val header = headerAndRows.first
  val mtcdata = headerAndRows.filter(_ (0) != header(0))

  // step 2
  val populationDF = mtcdata
    .map(p => (p(1), p(6)))
    .toDF("rank", "salary")

  // step 3
  println("==================== STEP 3 ====================")
  println()
  populationDF
    .groupBy(col("rank").as("Rank"))
    .agg(round(mean("salary"), 2).as("Mean"),
      round(variance("salary"), 2).as("Var"))
    .sort("Rank")
    .show()

  println("================= STEP 4, 5, 6 =================")
  println()
  // step 4, 5, 6
  val schema = StructType(Array(
    StructField("rank", StringType, nullable = false),
    StructField("salary", StringType, nullable = false)
  ))

  val samplingPercentage = args(0).toInt
  val iteration = args(1).toInt

  val aggProf = sampleAndAgg(populationDF, "Prof", samplingPercentage, iteration)
  val aggAssocProf = sampleAndAgg(populationDF, "AssocProf", samplingPercentage, iteration)
  val aggAsstProf = sampleAndAgg(populationDF, "AsstProf", samplingPercentage, iteration)

  Seq(("Prof", aggProf._1, aggProf._2), ("AssocProf", aggAssocProf._1, aggAssocProf._2), ("AsstProf", aggAsstProf._1, aggAsstProf._2))
    .toDF("Rank", "Mean", "Var")
    .sort("Rank")
    .show()

  def sampleAndAgg(df: sql.DataFrame, rank: String, samplingPercentage: Int, iteration: Int): (Double, Double) = {

   val sample = df
      .filter($"rank" === rank).rdd
      .takeSample(withReplacement = false, (df.count()*samplingPercentage/100).toInt)

    val sampleRDD = sc.makeRDD(sample)

    var meanSum, varSum = BigDecimal(0)
    for (_ <- 1 to iteration) {
      val resample = sampleRDD
        .takeSample(withReplacement = true, sample.length)

      val agg = spark.createDataFrame(sc.parallelize(resample), schema)
        .agg(mean("salary"),
          variance("salary"))
        .collect()

      meanSum += agg(0).getDouble(0)
      varSum += agg(0).getDouble(1)
    }

    ((meanSum / iteration).setScale(2, RoundingMode.HALF_UP).toDouble,
      (varSum / iteration).setScale(2, RoundingMode.HALF_UP).toDouble)
  }
}
