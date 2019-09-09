package com.ly.spark_graphx.main

import com.ly.spark_graphx.job.ContactGraph

object JobStart {
  def main(args: Array[String]): Unit = {
    val contactGraph = new ContactGraph
    contactGraph.startJob
    println("max-long: "+Long.MaxValue+"\n min-long: "+Long.MinValue)
  }

}
