package temputer

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object temputer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("weather")
    val ssc = new StreamingContext(conf, Seconds(6))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.80.107:9092,192.168.80.108:9092,192.168.80.109:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "234",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("weather")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val ds2 = stream.map(r => (r.key.substring(0,10),r.value().toInt))
    val ds3 = ds2.window(Seconds(20*6),Seconds(4*6))
    val ds4 = ds3.groupByKey()
    val ds5 = ds4.map(r => (r._1,r._2.sum*1.0/r._2.size))
//    ds5.print()
    val ds6 = ds5.map(r => (r._1,r._2 > 22))
//    ds6.print()

    val ds7 = ds6.reduce((a,b) => ((a._1+"="+b._1),(a._2 && b._2)))
    ds7.foreachRDD(x =>{
      x.foreach(r =>{
        val sql = "INSERT INTO temp(data,result) VALUE ('"+r._1+"','"+r._2+"');"
        JdbcHelper.update(sql)
      })
    })
    ds7.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

