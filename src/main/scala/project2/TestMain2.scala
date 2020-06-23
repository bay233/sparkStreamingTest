package project2

import Test1.Test3_KafkaStream.updateFunc
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project2.SpringMVC.Dao.MysqlManager
import project2.SpringMVC.service.{MysqlService, SegmentService}
import project2.SpringMVC.utiles.Conf
import project2.TestMain.log
import project2.wordSeqGenerator.BroadcastWrapper

import scala.collection.mutable.HashSet

object TestMain2 extends Serializable {
  @transient lazy val log = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer")
      .setMaster(Conf.master)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(Conf.interval))

    ssc.checkpoint("./chpoint")
    val topicsSet = Conf.topics.split(",").toSet
    // 设置Kafka抓取策略
    val kafkaParams = scala.collection.immutable.Map[String, Object]("bootstrap.servers" -> Conf.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> Conf.group,
      "enable.auto.commit" -> (true: java.lang.Boolean))

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )
    log.warn(s"初始化联接完成***>>>topic:${Conf.topics}   group:${Conf.group} localDir:${Conf.localDir} brokers:${Conf.brokers}")

    kafkaDirectStream.cache ///缓存

    val linesStream = kafkaDirectStream.map(_.value)

    val wordsStream = linesStream.flatMap(lines => {
      val words = lines.split(" ")
      words.map(v => (v, 1))
    })

    //val countedStream = wordsStream.reduceByKey(_+_)

    //val reduced: DStream[(String, Int)] = countedStream.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), true)

    val wordCounts = wordsStream.reduceByKeyAndWindow((a:Int,b:Int) => a+b, Seconds(10), Seconds(10))


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

  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])] ) => {
    iter.map{ case(x,y,z) => (x, y.sum + z.getOrElse(0))}
  }
}
