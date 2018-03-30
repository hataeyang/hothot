package com.taeyang

import org.apache.spark.sql.SparkSession

object example322 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var testArray = Array(22,33,50,70,90,100)
    var answer = testArray.filter(x=>{
    var data = x.toInt
     data%10==0

    })










  }
}


