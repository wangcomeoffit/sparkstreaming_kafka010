import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * * @Author wang guo
  * * @Create Date 2020/9/19 0:02
  */
object Streaming_Mysql {

  System.setProperty("hadoop.home.dir", "E:\\evn\\spark-2.2.0-bin-hadoop2.7")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_mysql")
    var ssc=new StreamingContext(conf,Seconds(6))

    //创建topic
    //var topic=Map{"test" -> 1}
    var topic=Array("test1")
    //指定zookeeper
    //创建消费者组
    var group: String ="con-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> "192.168.0.2:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );
    //创建DStream，返回接收到的输入数据
    var stream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
    val result:DStream[(String,Int)] = stream.map(_.value()).flatMap(_.split(" ")).map((_,1))
    //每一个stream都是一个ConsumerRecord
    //每分钟获取一个stream（DSTREAM是一个RDD)
    //val result = stream.print()

    result.print()

/*    result.foreachRDD( rdd=>{
      rdd.foreachPartition( it =>{
        //数据非空
        if(!rdd.isEmpty()){
          Class.forName("com.mysql.jdbc.Driver")
          //获取mysql连接
          val conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "root", "123")
          //把数据写入mysql
          try {
            for (row <- it) {
              val sql = "insert into wordcount(word,count)values('" + row._1 + "','" + row._2 + "')"
              conn.prepareStatement(sql).executeUpdate()
            }
          } finally {
            conn.close()
          }

        }
      }

      )
    }


    )*/

/*    //往mysql里面写数据  测试成功
    result.foreachRDD(rdd => rdd.foreachPartition(line => {
      Class.forName("com.mysql.jdbc.Driver")
      //获取mysql连接
      val conn = DriverManager.getConnection("jdbc:mysql://192.168.0.2:3306/test", "root", "123")
      //把数据写入mysql
      try {
        for (row <- line) {
          val sql = "insert into wordcount(word,count)values('" + row._1 + "','" + row._2 + "')"
          conn.prepareStatement(sql).executeUpdate()
        }
      } finally {
        conn.close()
      }
    }))*/


    ////连接池
    //第一段，foreach，一条数据连接一次MySQL，非常消耗资源
//        result.foreachRDD(rdd=>{
//    //这里是在driver端执行，跨网络需要序列化，这里会有序列化的问题，要放到foreach里面
//          //val connection=getConnection()
//          rdd.foreach(kv=>{ //foreache是在excutor端执行
//            val connection=getConnection()
//            val sql=s"insert into wordcount(word,count) values ('${kv._1}', '${kv._2}')"
//            connection.createStatement().execute(sql)
//            connection.close()
//          })
//        })

    //第二段,优化，foreachPartition，连接MySQL是一个高消耗的事情，一个分区连接一次
    //        result.foreachRDD(rdd => {
    //          rdd.foreachPartition(partionOfRecords => {
    //              val connection = getConnection()
    //              partionOfRecords.foreach(kv => {
    //                val sql = s"insert into wc(word,cnt) values ('${kv._1}', '${kv._2}')"
    //                connection.createStatement().execute(sql)
    //              })
    //              connection.close()
    //          })
    //        })

        //往mysql里面写数据  测试成功
      result.foreachRDD(rdd => {
        val config = ConfigFactory.load()
        //获取mysql连接
        val url = config.getString("db.url")
        val conn = new Properties()
        conn.setProperty("user", config.getString("db.user"))
        conn.setProperty("password", config.getString("db.password"))
        conn.setProperty("driver", config.getString("db.driver"))
        //把数据写入mysql
          rdd.foreachPartition(it => {
            val props=DriverManager.getConnection(url,conn)

            it.foreach{case (word,count)=>{
              val pstm1=props.prepareStatement("select count from wordcount where word=?")
              pstm1.setString(1,word)
              val set=pstm1.executeQuery()

              //有值  word 已经存在
              if(set.next()){
                val hisDat = set.getInt("count")
                //累计求和
                val newDat =count+hisDat

                val pstm2 = props.prepareStatement("update wordcount set count=? where word=?")
                pstm2.setInt(1,newDat)
                pstm2.setString(2,word)
                pstm2.executeUpdate()

              }else{//直接插入
                val pstm3=props.prepareStatement("insert into wordcount values(?,?)")
                pstm3.setString(1,word)
                pstm3.setInt(2,count)
                pstm3.executeUpdate()
              }

             }
            }
            conn.clone()

          })

      })




    ssc.start()
    //awaitTermination方法：接收人timeout和TimeUnit两个参数，用于设定超时时间及单位。当等待超过设定时间时，
    // 会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。一般情况下会和shutdown方法组合使用
    ssc.awaitTermination()

  }

  def getConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.0.2:3306/test?useSSL=false","root","123")
  }

}
