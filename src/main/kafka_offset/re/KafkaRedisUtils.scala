package re

/**
  * * @Author wang guo
  * * @Create Date 2020/9/16 13:58
  */
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}

object KafkaRedisUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
    * 创建 DirectStream
    * @param streamingContext
    * @param kafkaParams   kafka参数
    * @param module    模块名
    * @param groupId   消费者组
    * @param topics    topics
    * @return
    */
  def createDirectStream(streamingContext: StreamingContext, kafkaParams: Map[String, Object],
                         module: String, groupId: String,topics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {

    //读取 topic 的 offset
    val storedOffsets = readOffsets(module, groupId, kafkaParams, topics)

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = storedOffsets match {
      //上次未保存offsets
      case None => {
        KafkaUtils.createDirectStream[String, String](
          streamingContext,
          PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
      }
      case Some(fromOffsets) => {
        KafkaUtils.createDirectStream[String, String](
          streamingContext,
          PreferConsistent,
          // 指定分区消费，无法动态感知分区变化
          //          ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets)
        )
      }
    }
    kafkaStream
  }


  /**
    * 读取 offset
    * @param module
    * @param groupId
    * @param kafkaParams
    * @param topics
    * @return
    */
  def readOffsets(module: String, groupId: String,kafkaParams: Map[String, Object], topics: Array[String]): Option[Map[TopicPartition, Long]] = {
    logger.info("Reading offsets from Redis")
    val jedis = JedisPoolUtils.getPool.getResource

    //设置每个分区起始的Offset
    var fromOffSets: Map[TopicPartition, Long] = Map()
    try {
      topics.foreach(topic => {
        var topicFromOffsets: Map[TopicPartition, Long] = Map()
        val key = s"${module}:${groupId}:${topic}"
        if (jedis.exists(key)) {
          val offsetMap = jedis.hgetAll(key)
/*          offsetMap.map( record => {
            //判断field(即分区)是否存在
            if(jedis.hexists(key,record._1)){
              logger.info(s"topic ${topic} partition ${record._1} get lastSavedOffset from redis: ${record._2}")
              topicFromOffsets += new TopicPartition(topic, record._1.toInt) -> record._2.toLong
            }else{
              jedis.hset(key,record._1,"0")
            }
          })*/


        }
        fromOffSets ++= topicFromOffsets
      })
    } catch {
      case e: Exception => logger.error("readOffsets error ", e)
        System.exit(1)
    }finally {
      jedis.close()
    }
    if (fromOffSets.isEmpty) {
      None
    } else {
      Some(fromOffSets)
    }
  }


}
