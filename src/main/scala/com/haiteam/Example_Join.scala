package com.haiteam

import org.apache.spark.sql.SparkSession;



object Example_Join {



  //spark연동
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  // dataPath :경로설정   메인,서브 각각 설정
  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"

  //DataFrame
  var mainData1 = spark.read.format("csv").
    option("header", "true").load(dataPath + mainData)
  //DataFrame
  var subData1 = spark.read.format("csv").
    option("header", "true").load(dataPath + subData)


// (임시)테이블생성 일단가볍게 쓰는용도? 이리저리
  mainData1.createTempView("maindata")
  subData1.createTempView("subdata")


  var leftJoinData = spark.sql("select a.*, b.productname " +
    "from maindata a left outer join subdata b " +
    "on a.productgroup = b.productid")








}
