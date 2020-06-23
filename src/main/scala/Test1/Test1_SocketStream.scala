package Test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test1_SocketStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // 创建sparkStreaming 上下文
    // 线程数不能少于2
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    // 绑定一个端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 进行单词计数
    val words=lines.flatMap( _.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordsCounts = pairs.reduceByKey(_+_)

    wordsCounts.print()
    ssc.start()   // 启动sparkStreaming程序

    ssc.awaitTermination()  // 等待系统发出退出信号
  }

}
