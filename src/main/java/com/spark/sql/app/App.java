package com.spark.sql.app;

import com.spark.sql.udf.Calculator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
public static void main(String args[]){

    SparkConf conf = new SparkConf();
    conf.setAppName("Spark SQL");
    conf.setMaster("yarn");
    SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
    Dataset<Row> input = session.sql("select * from default.values");
    Calculator.process(session,input);
}
}
