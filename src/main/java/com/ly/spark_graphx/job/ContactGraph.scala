package com.ly.spark_graphx.job

import com.ly.spark_graphx.traits.SparkSessionTrait
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD

class ContactGraph extends SparkSessionTrait {

  /**
   * 创建图方式一
   *
   * @return
   */
  def createGraphFromRDD = {
    log("从RDD构建Graph")
    val sc = ss.sparkContext
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(
        Array(
          (3L, ("15239871100", "陆小凤")),
          (7L, ("15287679231", "花满楼")),
          (5L, ("15287842135", "西门吹雪")),
          (2L, ("15285461279", "司空摘星"))
        ))
    // Create an RDD for edges
    val relationships: RDD[Edge[(String, String)]] =
      sc.parallelize(
        Array(
          Edge(3L, 7L, ("电话", "2019-03-05 19:12:20")),
          Edge(5L, 3L, ("短信", "2019-03-08 09:20:05")),
          Edge(2L, 5L, ("电话", "2019-03-10 05:12:23")),
          Edge(5L, 7L, ("短信", "2019-05-21 22:20:45")),
          Edge(5L, 8L, ("短信", "2019-08-15 15:13:02"))
        ))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("None", "Nobody")
    // Build the initial Graph
    Graph(users, relationships, defaultUser)

  }

  /**
   * 创建图方式二：fromEdge
   */
  def createGraphFromEdge = {
    val relationships: RDD[Edge[(String, String)]] =
      ss.sparkContext.parallelize(
        Array(
          Edge(3L, 7L, ("电话", "2019-03-05 19:12:20")),
          Edge(5L, 3L, ("短信", "2019-03-08 09:20:05")),
          Edge(2L, 5L, ("电话", "2019-03-10 05:12:23")),
          Edge(5L, 7L, ("短信", "2019-05-21 22:20:45")),
          Edge(5L, 8L, ("短信", "2019-08-15 15:13:02"))
        ))
    val defaultUser = ("None", "Nobody")
    Graph.fromEdges(relationships, defaultUser)
  }

  /**
   * 创建图方式三：fromEdgeTules
   */
  def createGraphFromEdgeTuple = {
    val relationships: RDD[(Long, Long)] =
      ss.sparkContext.parallelize(
        Array(
          (3L, 7L),
          (5L, 3L),
          (2L, 5L),
          (5L, 7L),
          (5L, 8L)
        ))
    val defaultUser = ("None", "Nobody")
    Graph.fromEdgeTuples(relationships, defaultUser)
  }

  /**
   * 创建图方式四：fromEdgeListFile
   */

  def createGraphFromEdgeListFile = {
    val sc = ss.sparkContext
    //加载边时顶点是边上出现的点,定点默认数据是1，注意文件格式:1 2,
    // 中间是空格graphx只会读取两列分别作为源顶点和目标顶点,如:1 2 other，第三列的other直接被忽略
    val fileGraph = GraphLoader.edgeListFile(sc, "d:/data/edges.txt")

    val users = sc.textFile("d:/data/vertices.txt").map { line =>
      val fields = line.split(" ")
      (fields(0).toLong, (fields(1), fields(2))) //解析顶点数据:ID(一定转成Long型),fisrt name,full name
    }
    //由于graph默认将顶点数据设为1，
    // 将顶点数据users和边数据graph.edges重构为新图,
    // 如果边edges中的顶点A在顶点集合users中没有，
    // 则该顶点A将会以默认值初始化，可以添加默认值
    //    val myGraph=Graph.apply(users,fileGraph.edges)
    val defaultUsers = ("None", "Nobody")
    val myGraph2 = Graph.apply(users, fileGraph.edges, defaultUsers)
    myGraph2
  }
  /*
     pregel api 单源最短路径算法，计算该源点到图中每个顶点的最短路径
    */
  def calcShortestPath()={

    val contactGraph = createGraphFromRDD  //创建属性图
    val sourceId: VertexId = 2L //定义路径源点
    //初始化图中每个顶点的属性，增加路径长度属性，源顶点设置为0.0，其他全部为正无穷
    val initGraph = contactGraph
      .mapVertices((id,p)=>if(id == sourceId) (p,0.0) else (p,Double.PositiveInfinity))
      //每条边增加属性，单边路径长度默认为1
      .mapEdges(e =>(e.attr,1))
    val shotestPathGraph = initGraph
      .pregel(//第一个参数列表
        //每点的消息初始化默认值，正无穷
        Double.PositiveInfinity
      )(//第二个参数列表
        //迭代计算超步1.初始化顶点消息，取属性和消息的较小值
        (id,attr,msg) =>(attr._1,math.min(attr._2,msg)),
        triple =>{
          //迭代计算超步2
          // 计算源顶点到其他顶点 逻辑判断，其他非源顶点都不计算返回空，表示没有消息发送，顶点设为非活跃状态
          if(triple.srcAttr._2 + triple.attr._2 < triple.dstAttr._2){
            //计算当前顶点到目标顶点的路径+1，并发送该消息值到目标顶点，目标顶点设为活跃状态
            Iterator((triple.dstId,triple.srcAttr._2 +triple.attr._2))
          }else{
            Iterator.empty
          }
        },
        //迭代计算超步3.
        // 目标顶点接收所有来自源顶点的消息值，取最小的作为源顶点 -> 目标顶点最短路径
        (v1,v2) =>math.min(v1,v2)
      )
    println("单源最短路径算法结果：")
    shotestPathGraph.triplets.collect().foreach(println)
  }

  /**
   * graphX 图属性操作
   */
  def mapOperators()={
    val graph = createGraphFromRDD
    println("源通信关系图")
    graph.triplets.take(10).foreach(println)
    //mapVertices操作
    println("mapVertices操作：")
    graph
      //顶点属性只取手机号
      .mapVertices((vId,attr)=>(attr._1))
      .triplets
      .take(10)
      .foreach(println)
    println("mapEdges操作：")
    graph
      //只取通信类型
      .mapEdges(edge =>edge.attr._1)
      .triplets
      .take(10)
      .foreach(println)
    println("mapTriplets操作")
    graph
      .mapTriplets(triple =>triple.attr._1)
      .triplets
      .take(10)
      .foreach(println)
  }


  /**
   * job 定制任务
   */
  override protected def run(): Unit = {
    //图构建
    //    val contactGraph = createGraphFromRDD
    /*val contactGraph = createGraphFromEdgeListFile
    val triplet: RDD[EdgeTriplet[(String, String), Int]] = contactGraph.triplets
    triplet.collect().foreach(println)*/

    //单源最短路径算法 Pregel API
//    calcShortestPath()
    mapOperators()


  }
}
