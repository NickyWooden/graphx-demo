package com.ly.spark_graphx.traits

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTrait extends SparkLog{
  final val appName = "Spark-Job------>"+getClass.getSimpleName
  protected final var ss :SparkSession = _
  /**
    * SparkSession初始化
    */
  protected def init()={
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.shuffle.memoryFraction", "0.6")
      .set("spark.network.timeout", "300")
      .set("spark.shuffle.io.retryWait", "30s")
      .set("spark.shuffle.io.maxRetries", "12")

      //      .set("spark.shuffle.consolidateFiles", "true")
      //      .set("spark.shuffle.manager", "hash")
      .set("spark.reducer.maxSizeInFlight", "96MB")
      .set("spark.locality.wait", "9s")

      .set("spark.driver.memory","1g")
      .setMaster("local")
//      .setJars(List("D:\\IdeaWorkspace\\SparkSessionDemo\\target\\word-count.jar"))
//        .set("spark.authenticate","true")
//        .set("spark.authenticate.secret","123456")
//        .set("spark.acls.enable","false")
//      .set("spark.ui.view.acls","li yuan")
//      .set("spark.modify.acls","li yuan")

    ss = SparkSession.builder().enableHiveSupport().config(conf ).appName(appName)getOrCreate()
  }

  /**
    * job 定制任务
    */
  protected def run():Unit
  protected def stop ={
    if(ss !=null) ss.close()
  }

  /**
    * 开始执行job
    */
   def startJob = {

    log("The job name: "+appName)
    val start = System.currentTimeMillis()
    init()
    run()
    stop
    val end = System.currentTimeMillis()
    val runtime = (end - start)/1000
    log(s"job runtime : "+runtime+"s")
  }

}
