package re
//import re.{JedisPoolUtils, KafkaRedisUtils, RedisConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Pipeline
//https://blog.csdn.net/weixin_43215250/article/details/101773033?utm_medium=distribute.pc_relevant.none-task-blog-title-5&spm=1001.2101.3001.4242
/**
  * * @Author wang guo
  * * @Create Date 2020/9/16 14:05
  */
object SparkSaveOffsetToRedisApp {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\evn\\spark-2.2.0-bin-hadoop2.7")

    // Kafka 的 Offsets 以 module:groupId:topic 为 key 的 hash 结构存入 Redis 中
    val module: String = "Test"

    val groupId: String = "groupId-01"

    val topics: Array[String] = "my-topic".split(",")

    // sparkstreaming 消费 kafka 时的 Consumer 参数
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.1.100:9092,192.168.1.101:9092,192.168.1.102:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    // 初始化 Redis 连接池
    JedisPoolUtils.makePool(RedisConfig("192.168.1.100", 6379, 30000, 1000, 100, 50))

    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setAppName("Spark Save Offset To Zookeeper App")

    val streamingContext = new StreamingContext(conf, Seconds(30))
    val kafkaStream = KafkaRedisUtils.createDirectStream(streamingContext, kafkaParams, module, groupId, topics)

    //开始处理批次消息
    kafkaStream.foreachRDD(rdd => {
      //获取当前批次的RDD的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 处理从获取 kafka 中的数据
      if (!rdd.isEmpty()) {
        // 获取 redis 连接
        val jedisClient = JedisPoolUtils.getPool.getResource
        //开启事务
        val pipeline: Pipeline = jedisClient.pipelined()
        pipeline.multi()

        try {
          // 处理从获取 kafka 中的数据
          val result = rdd.map(_.value()).map(_.split("\\|\\|")).map(x => (x(0), x(1), x(2)))
          logger.info("==========> Total " + rdd.count() + " events in this    batch ..")

          result.foreach(println(_))

          //更新offset到Redis中
          offsetRanges.foreach({ offsetRange =>
            logger.info("==========> partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset
              + " untilOffset: " + offsetRange.untilOffset)

            // Kafka 的 Offsets 以 module:groupId:topic 为 key 的 hash 结构存入 Redis 中
            val key = s"${module}:${groupId}:${offsetRange.topic}"
            pipeline.hset(key, offsetRange.partition.toString, offsetRange.untilOffset.toString)
          })
          //提交事务
          pipeline.exec()
          //关闭pipeline
          pipeline.sync()
        } catch {
          case e: Exception => {
            logger.error("数据处理异常", e)
            pipeline.discard()
          }
        } finally {
          //关闭连接
          pipeline.close()
          jedisClient.close()
        }
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }

}
