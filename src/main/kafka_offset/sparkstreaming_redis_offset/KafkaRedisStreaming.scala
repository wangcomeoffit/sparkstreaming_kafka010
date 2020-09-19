package sparkstreaming_redis_offset
import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import redis_offset.InternalRedisClient
/**
  * * @Author wang guo
  * * @Create Date 2020/9/19 17:26
  */
object KafkaRedisStreaming {

  System.setProperty("hadoop.home.dir", "E:\\evn\\spark-2.2.0-bin-hadoop2.7")
  // 过滤日志
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("xx")
      //每秒钟每个分区kafka拉取消息的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 序列化
      .set("spark.serilizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(5))
    //启动一参数设置
    val groupId = "ts001"
    // kafka配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.2:9092",
      // kafka的key和value的解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 从头开始消费
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("test1")
    //启动二参数设置  （获取Redis中的kafka偏移量）
    var formdbOffset: Map[TopicPartition, Long] = JedisOffset(groupId)


    //拉取kafka数据
    val stream: InputDStream[ConsumerRecord[String, String]] =
    // 首先判断一下 我们要消费的kafka数据是否是第一次消费，之前有没有消费过
      if (formdbOffset.size == 0) {
        KafkaUtils.createDirectStream[String, String](
          ssc,
          /**
            * 本地策略
            * 一般使用LocationStrategies的PreferConsistent方法。
        它会将分区数据尽可能均匀地分配给所有可用的Executor。
        题外话：本地化策略看到这里就行了，下面讲的是一些特殊情况。
        情况一
        如果你的Executor和kafka broker在同一台机器上，可以用PreferBrokers，
        这将优先将分区调度到kafka分区leader所在的主机上。
        题外话：废话，Executor是随机分布的，我怎么知道是不是在同一台服务器上？
        除非是单机版的are you明白？
        情况二
        分区之间的负荷有明显的倾斜，可以用PreferFixed。
        这个允许你指定一个明确的分区到主机的映射（没有指定的分区将会使用连续的地址）。
        题外话：就是出现了数据倾斜了呗
            */
          LocationStrategies.PreferConsistent,
          /**
            * 消费者策略
            * ConsumerStrategies.Subscribe，能够订阅一个固定的topics的集合。
            * SubscribePattern 能够
            * 根据你感兴趣的topics进行匹配。需要注意的是，不同于 0.8的集成，
            * 使用subscribe or SubscribePattern 可以支持在运行的streaming中增加分区。
            * 而Assign不可以动态的改变消费的分区模式，那么一般都会在开始读取固定的数据时候才能使用
            */
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
      } else {

        // 第一次消费数据，没有任何的消费信息数据
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String, String](
            formdbOffset.keys, kafkaParams, formdbOffset)
        )
      }


    //数据偏移量处理。
    stream.foreachRDD({
      rdd =>
        // 获得偏移量对象数组
        val offsetRange: Array[OffsetRange] =
          rdd.asInstanceOf[HasOffsetRanges].offsetRanges



        // 偏移量存入redis
        val jedis: Jedis = JedisConnectionPool.getConnection()
        val jedis1 = JedisConnectionPool.getPool.getResource
        val p = jedis1.pipelined()
        p.multi() //开启事务

        //逻辑处理
        rdd.map(_.value())
          .map((_, 1))
          .reduceByKey(_ + _).foreach(println)


        for (or <- offsetRange) {
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }



        p.exec() //提交事务
        p.sync //关闭pipeline
        jedis.close()
    })
    // 启动Streaming程序
    ssc.start()
    ssc.awaitTermination()
  }

}
