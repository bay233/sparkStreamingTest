package project1

import java.util.Properties

import Test1.Test3_KafkaStream.updateFunc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
1. 日志的分析
   a)有状态的分析:
        ssc.checkpoint("./chpoint")
        //  [级别]\t位置\t信息                                    ( 级别,1)
    	val cached=logStream.map(_.split("\t")).map(   arr=>(arr(0),1)).cache()
        val result=cached.updateStateByKey( updateFunc, new HashPartitioner(  ssc.sparkContext.defaultMinPartitions  ),  true )
        加入更新状态的函数
          val updateFunc= (   iter:Iterator[ (String,Seq[Int] ,  Option[Int] ) ] ) =>{
    			//方案一:当成一个三元组运算
    			// iter.map(    t=> (  t._1,   t._2.sum+t._3.getOrElse(0)  )    )   //  ->  { word：总次数}
    			//方案二: 模式匹配来实现
    			iter.map{  case(x,y,z)=>(  x,  y.sum+z.getOrElse(0)   ) }
  			}
    b)window操作:   批处理间隔2秒   窗口的长度4秒  滑动时间间隔2秒
        val r2=cached.reduceByKeyAndWindow( (a:Int,b:Int)=>a+b, Seconds(4)  , Seconds( 2) )
         //定义一个打印函数，打印RDD中所有的元素
		  def printValues(stream: DStream[(String, Int)]) {     //    DStream -> n个RDD组成  -> 一个RDD由n 条记录组成  -》一条记录由  (String, Int) 组成
		    stream.foreachRDD(foreachFunc) //   不要用foreach()  ->  foreachRDD
		    def foreachFunc = (rdd: RDD[(String, Int)]) => {
		      val array = rdd.collect()   //采集 worker端的结果传到driver端.
		      println("===============window窗口===============")
		      for (res <- array) {
		        println(res)
		      }
		      println("===============window窗口===============")
		    }
		  }
 */

case class Record(log_level: String, method: String, content: String)

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val sparkConf = new SparkConf()
      .setAppName("LogAnalysis")
      .setMaster("local[*]")
    //因为要用到spark sql , 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("LogAnalysis")
      .config(sparkConf)
      .getOrCreate()
    //利用sparkSession创建上下文
    val sc = spark.sparkContext
    //建立流式处理上下文  spark Streaming
    val ssc = new StreamingContext(sc, Seconds(2))

    // jdbc配置
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "a")

    val logStream = ssc.textFileStream("data\\project1\\logs")
    ssc.checkpoint(".\\chpoint") // window操作与有状态的操作

    logStream.foreachRDD((rdd: RDD[String]) => {
      import spark.implicits._

      val data = rdd.map(w => {
        val takens = w.split("\t")
        Record(takens(0), takens(1), takens(2))
      }).toDF()

      data.createOrReplaceTempView("alldata")

      // 条件筛选：只查看 error 和 warn信息
      val logImp = spark.sql("select * from alldata where log_level='[error]'")
      logImp.show()

      // 输出到外部Mysql中
      logImp.write.mode(SaveMode.Append) // 保存模式，这里是选择追加添加
        .jdbc("jdbc:mysql://localhost:3306/spark_test", "important_logs", properties)

    })

    val cached = logStream.map(_.split("\t")).map(arr => (arr(0),1)).cache()
    val result = cached.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), true)
    result.print()

    val r2=cached.reduceByKeyAndWindow( (a:Int,b:Int)=>a+b, Seconds(4)  , Seconds( 2) )
    //r2.print()
    printValues(  r2 )


    ssc.start()
    ssc.awaitTermination()
  }
  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])] ) => {
    // 方案一
    //iter.map( t => (t._1, t._2.sum+t._3.getOrElse(0)))

    // 方案二：模式匹配
    iter.map{ case(x,y,z) => (x, y.sum + z.getOrElse(0))}

  }

  //定义一个打印函数，打印RDD中所有的元素
  def printValues(stream: DStream[(String, Int)]) { //    DStream -> n个RDD组成  -> 一个RDD由n 条记录组成  -》一条记录由  (String, Int) 组成
    stream.foreachRDD(foreachFunc) //   不要用foreach()  ->  foreachRDD
    def foreachFunc = (rdd: RDD[(String, Int)]) => {
      val array = rdd.collect() //采集 worker端的结果传到driver端.
      println("===============window窗口===============")
      for (res <- array) {
        println(res)
      }
      println("===============window窗口===============")

    }
  }

}


