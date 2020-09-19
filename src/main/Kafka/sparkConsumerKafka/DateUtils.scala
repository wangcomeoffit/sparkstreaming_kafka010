package spark





import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/*
* 日期时间工具类
* */

object DateUtils {

  val YYYYmmddhhmmss_FORMAT =FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT =FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String): Unit ={
    YYYYmmddhhmmss_FORMAT.parse(time).getTime

  }
  def parseToMinute(time:String)={
    TARGE_FORMAT.format(new Date())

  }

 /* def fun(time:String)={

    TARGE_FORMAT.format(
    new Date(getTime(time)))

  }*/
 def main(args: Array[String]): Unit = {
   print(getTime("2019-06-28 16:58:55"))
   print(parseToMinute("2019-06-28 16:58:55"))

   //parseToMinute("2019-06-28 16:58:55")

 }


}
