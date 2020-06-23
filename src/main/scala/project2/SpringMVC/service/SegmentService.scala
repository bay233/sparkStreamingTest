package project2.SpringMVC.service

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, LogManager, Logger}
import project2.SpringMVC.utiles.Conf
import scalaj.http.Http
import spray.json._

import scala.collection.mutable.HashSet
import scala.collection.mutable.Map

object SegmentService extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ALL) //配置日志
    val record="今天是星期天,今天天气是不错"
    val wordDic=new HashSet[String]()
    wordDic.add( "今天")
    wordDic.add("星期天")
    val map=mapSegment(   record, wordDic)
    print( map )

  }

  /*
    * 将sentence发给分词服务器，获取返回的切分好的中文词汇，然后再到 userDict 查是否有这个词，如果有，则统计数据量.
    * 在Dstream中调用，传入记录内容和词典，通过retry(3)(segment(postUrl, record))实现失败重试3次方案，根据分词结果和词典，进行词典指定词的词频统计，并以Map[Word,count]返回
    *
    */
  def mapSegment(record: String, wordDic: HashSet[String]): Map[String, Int] = {
    val preTime = System.currentTimeMillis()
    val keyCount = Map[String, Int]()
    if (record == "" || record.isEmpty) {
      log.warn(s"待切分语句为空: ${record}")
      keyCount
    } else {
      val postUrl = Conf.segmentorHost + "/token/"

      try {
        val wordsSet = retry(3)(segment(postUrl, record))
        log.warn(s"[拆分成功] 记录: ${record}\t耗时: ${System.currentTimeMillis - preTime}")

        for (word <- wordsSet) {
          if (wordDic.contains(word)) {
            keyCount += word -> 1
          }
        }
        log.warn(s"[keyCountSuccess] words size: ${wordDic.size} (entitId_createTime_word_language, 1):\t${keyCount.mkString("\t")}")
        keyCount

      } catch {
        case e: Exception => {
          log.error(s"[mapSegmentApiError] mapSegment error\tpostUrl: ${postUrl}${record}", e)
          keyCount
        }
      }
    }

  }


  /*
      根据传入的 url和 content发送请求，并解析返回的结果，将分词结果以HashSet形式返回
      参数: url:  http://localhost:8282/token/
            content:   今天是星期天
      返回值:   从jsonr的terms中取,按" "切分，形成数组 HashSet   ->  scala集合分两类: mutable,immutable
                HashSet: mutable
   */
  def segment(url: String, content: String): HashSet[String] = {
    val timer = System.currentTimeMillis()
    //地址栏的参数编码
    val c = URLEncoder.encode(content, StandardCharsets.UTF_8.toString)
    log.info("发送的请求为:" + url + "\t" + content + "\t" + c)

    var response = Http(url + c).header("Charset", "UTF-8").charset("UTF-8").asString
    log.info("响应为:" + response.code + "\t内容:" + response.body.toString)

    val dur = System.currentTimeMillis() - timer

    if (dur > 20) // 输出耗时较长的请求
      log.warn(s"[longVisit]>>>>>> api: ${url}${content}\ttimer: ${dur}")

    val words = new HashSet[String]
    response.code match {
      case 200 => {
        response.body.parseJson.asJsObject.getFields("ret", "msg", "terms") match {
          case Seq(JsNumber(ret), JsString(msg), JsString(terms)) => {
            if (ret.toInt != 0) {
              log.error(s"[segmentRetError] vist api: ${url}?content=${content}\tsegment error: ${msg}")
              words
            } else {
              //解析到了 单词组，则切分
              val tokens = terms.split(" ")
              tokens.foreach(token => {
                words += token
              })
              words
            }
          }
          case _ => words
        }
      }
      case _ => {
        //响应码为其它则异常,返回空words
        log.error(s"[segmentResponseError] vist api: ${url}?content=${content}\tresponse code: ${response.code}")
        // [segmentResponseError] vist api: http://localhost:8282/content=xxxx\tresponse code: 500
        words
      }
    }
  }


  // 重试函数
  def retry[T](n: Int)(fn: => T): T = {
    util.Try {
      fn
    } match {
      case util.Success(x) => {
        log.info(s"第${4 - n}次请求")
        x
      }
      case _ if n > 1 => {
        log.warn(s"[重试第 ${4 - n}次]")
        retry(n - 1)(fn)
      }
      case util.Failure(e) => {
        log.error(s"[segError] 尝试调用API失败了三次", e)
        throw e
      }
    }

  }


}
