package com.taeyang

import org.apache.spark.sql.SparkSession

object ldviubcubug {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1522/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle.show()


  }
}