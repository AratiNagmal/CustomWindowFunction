package com.spark.sql.udf

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Calculator {

  def process(session:SparkSession, input:DataFrame) = {

    val windows = Window.partitionBy("key1","key2","key3").orderBy("orderkey")
    input.withColumn("currentCount", CountWindowFunction.getCurrentCount(col("number")) over windows)
      .show(false)
  }
}
