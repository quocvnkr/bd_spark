package learning

import org.apache.spark.{SparkConf, SparkContext}

object SparkDataFrame extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // Exploring SparkSQL
    // Initialize an SQLContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Load a cvs file
    val csv = sc.textFile("mtcars.csv")
    // Create a Spark DataFrame
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_ (0) != header(0))
    val mtcars = mtcdata
      .map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
      .toDF
    mtcars.printSchema
    mtcars.select("mpg").show(5)
    mtcars.filter(mtcars("mpg") < 18).show()
    // Aggregate data after grouping by columns
    import org.apache.spark.sql.functions._
    mtcars.groupBy("cyl").agg(avg("wt")).show()
    mtcars.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()
    // Operate on columns
    mtcars.withColumn("wtTon", mtcars("wt") * 0.45).select(("car"), ("wt"), ("wtTon")).show(6)
    // Run SQL queries from the Spark DataFrame
    mtcars.registerTempTable("cars")
    val highgearcars = sqlContext.sql("SELECT car, gear FROM cars WHERE gear >= 5")
    highgearcars.show()

    // DataFrame from Scala objects
    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
    val infoRDD = sc.parallelize(info)
    val people = infoRDD.map(r => Person(r._1, r._2)).toDF()
    people.registerTempTable("people")
    val subDF = sqlContext.sql("select * from people where age > 30")
    subDF.show()

  }
}
