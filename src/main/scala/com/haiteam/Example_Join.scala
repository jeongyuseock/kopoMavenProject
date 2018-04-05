package com.haiteam

import org.apache.spark.sql.SparkSession;



object Example_Join {

  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"

  //DataFrame
  var mainData1 = spark.read.format("csv").
    option("header", "true").load(dataPath + mainData)
  //DataFrame
  var subData1 = spark.read.format("csv").
    option("header", "true").load(dataPath + subData)
// (가상) 테이블생성
  mainData1.createTempView("maindata")
  subData1.createTempView("subdata")

  var leftJoinData = spark.sql("select a.*, b.productname " +
    "from maindata a left outer join subdata b " +
    "on a.productgroup = b.productid")








}
