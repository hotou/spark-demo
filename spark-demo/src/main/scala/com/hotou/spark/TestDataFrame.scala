package com.hotou.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{Row, SQLContext}

object TestDataFrame {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Test Spark", System.getenv("SPARK_HOME"),
      SparkContext.jarOfClass(this.getClass).toSeq)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Create a bunch of rows
    val df1 = sqlContext.range(0, 100, 1, 4)
    df1.show()

    // Create random dataset
    val df2 = df1
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
    df2.show()

    // Test window function
    val getType = sqlContext.udf.register("getType", (i: Long) => {if(i % 2 == 0) "even" else "odd"})
    val df2_1 = df2.withColumn("type", getType($"id"))
    df2_1.show()
//    val w = Window.partitionBy("type").orderBy("id")
//    df2_1.select(
//      sum("normal").over(w.rangeBetween(Long.MinValue, 2)),
//      avg("uniform").over(w.rowsBetween(0, 4))
//    ).show()


    // See statistics of dataset
    df2.describe("uniform", "normal").show()

    // Select statistics of datasets
    df2.select(mean("uniform"), min("uniform"), max("uniform")).show()

    // Covariance and Correlation
    println(df2.stat.cov("uniform", "normal"))
    println(df2.stat.cov("id", "id"))

    println(df2.stat.corr("uniform", "normal"))
    println(df2.stat.corr("id", "id"))

    // Cross tab
    val names = Array("Alice", "Bob", "Mike")
    val items = Array("milk", "bread", "butter", "apples", "oranges")
    val rdd = sc.parallelize((1 to 100).map(x => Row(names(x % 3), items(x % 5))))
    val schema = StructType(Array(StructField("name", StringType), StructField("item", StringType)))
    val df3 = sqlContext.createDataFrame(rdd, schema)
    df3.show()
    df3.stat.crosstab("name", "item").show()

    // Frequent Items (with false positives)
    val rdd4 = sc.parallelize((0 to 99).map(i => if(i % 2 == 0) Row(1,2,3) else Row(i,i*2,i%4)))
    val schema4 = StructType(Array("a", "b", "c").map(StructField(_, IntegerType)))
    val df4 = sqlContext.createDataFrame(rdd4, schema4)
    df4.show(101)
    df4.stat.freqItems(Seq("a", "b", "c"), 0.4).show()

    // Math Functions
    val df5 = sqlContext.range(0, 10).withColumn("uniform", rand(seed=10) * 3.14)
    df5.select($"uniform", toDegrees("uniform"), pow(cos($"uniform"), 2) + pow(sin($"uniform"), 2)).show()
  }
}