package project2.wordSeqGenerator

import java.io.{File, FileInputStream, InputStream}
import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.parquet.io.InputFile

import scala.io.Source

/*
创建主题
kafka-topics.sh --create --zookeeper bay1:2181,bay2:2181,bay3:2181 --replication-factor 3 --partitions 3 --topic sparkStreamWordSeq
消费
kafka-console-consumer.sh --bootstrap-server bay1:9092,bay2:9092,bay3:9092  --topic  sparkStreamWordSeq  --from-beginning
 */

object WordSeqProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "bay1:9092,bay2:9092,bay3:9092")
    props.setProperty("acks", "all")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topic = "sparkStreamWordSeq"
    val random = new Random()
    val events = 10

    val t = System.currentTimeMillis();
    val producer = new KafkaProducer[String, String](props)
    val source = Source.fromFile("data\\project2\\hanzi.txt")
    val lines = try source.mkString finally source.close()

    for (nameAddr <- Range(0, events)) {

      // 截取文本内容 没条消息最多200个字符
      val sb = new StringBuilder()
      for (ind <- Range(0, random.nextInt(200))) {
        sb += lines.charAt(random.nextInt(lines.length))
      }
      // 随机命名用户
      val userName = "user_" + random.nextInt(100)
      val pr = new ProducerRecord[String, String](topic, userName, sb.toString())
      producer.send(pr)
    }

    System.out.println("每秒可以发送消息: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()
  }

}
