package com.ly.spark_graphx.traits

trait SparkLog {
  protected def log(msg :String) = {
    println("SparkLog===========> "+msg)
  }

}
