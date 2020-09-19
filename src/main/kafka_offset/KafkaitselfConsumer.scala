
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


/**
  * * @Author wang guo
  * * @Create Date 2020/9/16 10:03
  */
object KafkaitselfConsumer {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\evn\\spark-2.2.0-bin-hadoop2.7")
    var conf=new SparkConf().setMaster("local[*]").setAppName("Kafkaitself")
    var ssc=new StreamingContext(conf,Seconds(6))

    ssc.sparkContext.setLogLevel("WARN")

    val TOPIC= "test1"
    val groupId = "spark_test2_group"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest", // 初次启动从最开始的位置开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean) // 自动提交设置为 false
    )

    val topics = Array(TOPIC)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,//均匀分发到executor
      Subscribe[String, String](topics, kafkaParams)
    )


    stream.foreachRDD({ rdd =>

      println(s"-----rdd.partitions.size------------------ ${rdd.partitions.size}")

      // 获取当前批次的offset数据
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

      //TODO ... 业务处理

      // 在kafka 自身维护提交
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })
    ssc.start()
    ssc.awaitTermination()
  }

}
