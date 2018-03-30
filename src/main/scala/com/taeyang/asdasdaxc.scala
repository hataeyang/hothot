package com.taeyang

import org.apache.spark.sql.SparkSession

object asdasdaxc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_batch_season_mpara"



    // jdbc (java database connectivity) 연결
    val sun= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    sun.createOrReplaceTempView("selloutTable")
    var outputUrl = "jdbc:oracle:thin:@192.168.110.12:1522/XE"
    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var outputUser = "HZ"
    var outputPw = "manager"

    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user",outputUser)
    prop.setProperty("password",outputPw)
    var table ="pgParamTable"
      //append
      sun.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("success")
    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size

    for(i <-0 until priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
    }
 println(priceData)
var a = Array("컵","접시","숟가락","냄비","젓");
    var a1 = a.size;
    var b = "kopo_";
    for(i2<-0 to a1){
    var ab = b + a(i2)
      println(ab)
    }

    var testArray = Array("컵","접시","숟가락","냄비")
    var testArraySize = testArray.size

    for( i<-0 until testArraySize){
      a(i) = b+a(i)
      print(a(i))
    }



def discountedPrice(price:Double,rate:Double):Double ={
  var discount = price * rate
  var returnValue = price - discount
  returnValue
}
    var orgrRate = 0.2
    var orgPrice = 2000
    var newrPrice =
      discountedPrice(orgPrice, orgrRate)

  }

}
