package Test1

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object Test4_streamGetDataFromKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer") // 加入一个spark的序列化工具
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
      Subscribe[String, String](topics, kafkaParams)
    )

    val rdds = stream.cache()  // 不添加缓存则会造成重复消费

    val nameAddrsDStream = rdds.map(_.value).filter(record => {
      val items = record.split("\t")
      items(2).toInt == 0
    }).map(record => {
      val items = record.split("\t")
      (items(0), items(1))
    })

    val namePhonesDStream = rdds.map(_.value).filter(record => {
      val items = record.split("\t")
      items(2).toInt == 1
    }).map(record => {
      val items = record.split("\t")
      (items(0), items(1))
    })

    val result = nameAddrsDStream.join(namePhonesDStream)
      .map(v => (v._1, v._2._1, v._2._2))

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
