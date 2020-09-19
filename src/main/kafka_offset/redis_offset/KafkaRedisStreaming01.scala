package redis_offset

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


/**
  * * @Author wang guo
  * * @Create Date 2020/9/19 10:53
  */
object KafkaRedisStreaming01 {

  private val LOG = LoggerFactory.getLogger("KafkaRedisStreaming")
  System.setProperty("hadoop.home.dir", "E:\\evn\\spark-2.2.0-bin-hadoop2.7")
  //private val LOG = Logger.getLogger("org").setLevel(Level.ERROR)

  def initRedisPool() = {
    // Redis configurations
    val maxTotal = 20
    val maxIdle = 20
    val minIdle = 5
    val redisHost = "192.168.0.2"
    val redisPort = 6379
    val redisTimeout = 30000
    InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
  }

  /**
    * 从redis里获取Topic的offset值
    *
    * @param topicName
    * @param partitions
    * @return
    */
  def getLastCommittedOffsets(topicName: String, partitions: Int): Map[TopicPartition, Long] = {
        if (LOG.isInfoEnabled())
          LOG.info("||--Topic:{},getLastCommittedOffsets from Redis--||", topicName)

    //从Redis获取上一次存的Offset
    val jedis = InternalRedisClient.getPool.getResource
    val fromOffsets = collection.mutable.HashMap.empty[TopicPartition, Long]
    for (partition <- 0 to partitions - 1) {
      val topic_partition_key = topicName + "_" + partition
      val lastSavedOffset = jedis.get(topic_partition_key)
      val lastOffset = if (lastSavedOffset == null) 0L else lastSavedOffset.toLong
      fromOffsets += (new TopicPartition(topicName, partition) -> lastOffset)
    }
    jedis.close()

    fromOffsets.toMap
  }

  def main(args: Array[String]): Unit = {

    //初始化Redis Pool
    initRedisPool()

    var conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_Kafkait_redis")
    var ssc = new StreamingContext(conf, Seconds(6))

    val bootstrapServers = "192.168.0.2:9092"
    val groupId = "kafka-test-group"
    val topicName = "test1"
    val maxPoll = 20000

    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 这里指定Topic的Partition的总数
    val fromOffsets = getLastCommittedOffsets(topicName, 4)

    // 初始化KafkaDS
    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParam, fromOffsets))


    kafkaTopicDS.foreachRDD(rdd => {
      rdd.foreach(println)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 如果rdd有数据
      if (!rdd.isEmpty()) {
        val jedis = InternalRedisClient.getPool.getResource
        val p = jedis.pipelined()
        p.multi() //开启事务

        // 处理数据
        rdd
          .map(_.value)
          .flatMap(_.split(" "))
          .map(x => (x, 1L))
          .reduceByKey(_ + _)
          .sortBy(_._2, false)
          .foreach(println)

        //更新Offset
        offsetRanges.foreach { offsetRange =>
          println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
          val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
          p.set(topic_partition_key, offsetRange.untilOffset + "")
        }

        p.exec() //提交事务
        p.sync //关闭pipeline
        jedis.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
