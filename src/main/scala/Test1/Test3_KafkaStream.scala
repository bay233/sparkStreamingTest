package Test1

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/*
  需求：从Kafka读取数据, 累计
 */
object Test3_KafkaStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    ssc.checkpoint(".\\chpoint")


    // 当前是consumer端，要反序列化消息
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bay1:9092,bay2:9092,bay3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streamingAAA",   // 消费者组号
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)  // 提交位移的方式
    )

    // 订阅的主题
    val topics = Array("topicAAA")

    // 创建DStream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams) //SubscribePattern：正则表达式，Subscribe：固定，Assign：固定分区
    )

    // ConsumerRecord
    val lines: DStream[String] = stream.map(record => (record.value))
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    //val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), true)

    reduced.print()
    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * iter: 当前操作的RDD
    * String： 聚合的key
    * Seq[Int]：在这个批次中此key在这个分区出现的次数集合
    * Option[Int]：初始值或累加值， Some None -> 模式匹配
    */
  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])] ) => {
    // 方案一
    //iter.map( t => (t._1, t._2.sum+t._3.getOrElse(0)))

    // 方案二：模式匹配
    iter.map{ case(x,y,z) => (x, y.sum + z.getOrElse(0))}

  }
}
