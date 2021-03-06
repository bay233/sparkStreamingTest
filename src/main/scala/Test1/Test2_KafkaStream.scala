package Test1

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
/*
   3. 关闭:  sh kafka-server-stop.sh
                  sh zookeeper-server-stop.sh


3. 主题的相关操作:
   创建主题:
       bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 3  --partitions 3 --topic  topicAAA
   主题列表:  bin/kafka-topics.sh --list --zookeeper localhost:2181
   查看主题中消息详情: bin/kafka-topics.sh --describe --zookeeper localhost:2181    --topic topicAAA
   发送消息: bin/kafka-console-producer.sh --broker-list bay1:9092,bay2:9092,bay3:9092 --topic topicAAA
   消费消息:
     bin/kafka-console-consumer.sh --bootstrap-server bay1:9092,bay2:9092,bay3:9092  --topic   topicAAA  --from-beginning
 */

/*
  需求：从Kafka读取数据
 */
object Test2_KafkaStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

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
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    reduced.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
