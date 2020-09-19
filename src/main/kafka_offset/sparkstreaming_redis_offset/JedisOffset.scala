package sparkstreaming_redis_offset
import java.util
import org.apache.kafka.common.TopicPartition

/**
  * * @Author wang guo
  * * @Create Date 2020/9/19 17:25
  */
object JedisOffset {
  def apply(groupId: String) = {
    // 创建Map形式的Topic、partition、Offset
    var formdbOffset = Map[TopicPartition, Long]()
    //获取Jedis连接
    val jedis1 = JedisConnectionPool.getConnection()
    // 查询出Redis中的所有topic partition
    val topicPartitionOffset: util.Map[String, String] = jedis1.hgetAll(groupId)
    // 导入隐式转换
    import scala.collection.JavaConversions._
    // 将Redis中的Topic下的partition中的offset转换成List
    val topicPartitionOffsetlist: List[(String, String)] =
      topicPartitionOffset.toList
    // 循环处理所有的数据
    for (topicPL <- topicPartitionOffsetlist) {
      val split: Array[String] = topicPL._1.split("[-]")
      formdbOffset += (
        new TopicPartition(split(0), split(1).toInt) -> topicPL._2.toLong)
    }
    formdbOffset
  }

}
