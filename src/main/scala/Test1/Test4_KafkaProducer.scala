package Test1

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/*
  生产消息到Kafka
  两种类型：  姓名+地址 name Addr  姓名+电话  name Phone
 */
object Test4_KafkaProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties();
    props.setProperty("bootstrap.servers", "bay1:9092,bay2:9092,bay3:9092");
    props.setProperty("acks", "all");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val topic = "topicAAA"

    val producer = new KafkaProducer[String,String](props)

    val nameAddrs = Map("smith"->"湖南", "tom"->"湖北","john"-> "湖南")
    val namePhones = Map("smith"->"11111111111","tom"->"22222222222","john"->"33333333333","tim"->"88888888888")

    val rnd = new Random()

    for (nameAddr <- nameAddrs){
      val pr = new ProducerRecord[String,String](topic,nameAddr._1,s"${nameAddr._1}\t${nameAddr._2}\t0")
      producer.send(pr)
      Thread.sleep(rnd.nextInt(1000)+1000)
    }


    for (namePhone <- namePhones){
      val pr = new ProducerRecord[String,String](topic,namePhone._1,s"${namePhone._1}\t${namePhone._2}\t1")
      producer.send(pr)
      Thread.sleep(rnd.nextInt(1000)+1000)
    }

    producer.close()

  }
}
