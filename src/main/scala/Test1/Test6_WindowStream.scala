package Test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test6_WindowStream {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    //  为什么这个线程不能少于2????
    val conf = new SparkConf().setMaster("local[*]").setAppName("saveResultToText")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint(".\\chpoint") // window操作与有状态的操作

    val lines = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(x => x.split(" "))

    // TODO: 采用reduceByKeyAndWindow操作进行叠加处理，窗口时间间隔与滑动时间间隔
    // 每10秒统计前30秒各单词累计出现的次数
    val wordCounts = words.map(word => (word,1)).reduceByKeyAndWindow((a:Int,b:Int) => a+b, Seconds(30), Seconds(10))

    printValues(wordCounts)
    ssc.start()
    ssc.awaitTermination()
  }

  //定义一个打印函数，打印RDD中所有的元素
  def printValues(stream:DStream[(String,Int)]): Unit ={
    stream.foreachRDD(foreachFunc)
    def foreachFunc=(rdd:RDD[(String,Int)])=>{
      val array = rdd.collect()  //采集 worker端的结果传到driver端.
      println("=====begin to show results =====")
      for (res <- array) {
        println(res)
      }
      println("=====ending  show results=====")
    }
  }

}
