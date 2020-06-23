package project2.wordSeqGenerator

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

case class BroadcastWrapper[T: ClassTag](
                                          @transient private val ssc: StreamingContext,
                                          @transient private val _v: T) {
  // 创建广播变量
  @transient private var v = ssc.sparkContext.broadcast(_v)

  /**
    * 广播变量是只读的，可以利用spark的unpersist()，它按照LRU( lease Recently used)最近最久没有使用原则删除老数据。
    */
  def update(newValue: T, blocking: Boolean = false): Unit = {
    v.unpersist(blocking) //删除缓存
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value: T = v.value //对外提供一个函数用于访问这个广播变量 , 体现了封装

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject().asInstanceOf[Broadcast[T]]
  }

}
