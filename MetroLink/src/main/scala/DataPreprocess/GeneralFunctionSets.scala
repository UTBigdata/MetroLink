package DataPreprocess

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

object GeneralFunctionSets {
  def hourOfDay_long(t: Long): Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeString = dateFormat.format(t * 1000)
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.HOUR_OF_DAY)
  }

  def dayOfMonth_long(t: Long): Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeString = dateFormat.format(t * 1000)
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  def dayOfYear_long(t: Long): Int = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeString = dateFormat.format(t * 1000)
    val time = dateFormat.parse(timeString)
    val calendar = Calendar.getInstance()
    calendar.setTime(time)
    calendar.get(Calendar.DAY_OF_YEAR)
  }

  def transTimeToTimestamp(timeString: String): Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.parse(timeString).getTime / 1000
    time
  }

  def transTimeToString(timeStamp: Long): String = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val time = dateFormat.format(timeStamp * 1000)
    time
  }

  //提取时间戳对应当天的秒数
  def secondsOfDay(t: Long): Long = {
    val date=transTimeToString(t).split(" ")(0)
    t-transTimeToTimestamp(date+" 00:00:00")
  }

}
