package cloud.maxzhao.d_Window

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *窗口api测试
 * @author Max
 * @date 2021/4/9 0:01
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //指定时间语义，采用event time
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //默认的watermark 200ms
//    env.getConfig.setAutoWatermarkInterval(5000L)

    val dataStream = env.addSource(new DataSource)
      //添加水位线和时间标记
//      .assignTimestampsAndWatermarks(new MyAssigner)
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DataReading](Time.seconds(8)) {
//        override def extractTimestamp(t: DataReading): Long = t.time.toLong * 1000
//      })


    val minValuePerWindowStream = dataStream.map(data => (data.info, data.value))
      .keyBy(0)
      .timeWindow(Time.seconds(15),Time.seconds(5))
      .min(1)

    minValuePerWindowStream.print().setParallelism(1)

    env.execute()

  }

}

class MyAssigner extends AssignerWithPeriodicWatermarks[DataReading]{
  val bound: Long = 1 * 1000 // 延时为 10s
  var maxTs: Long = Long.MinValue // 观察到的最大时间戳

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(t: DataReading, l: Long): Long = {
    maxTs = maxTs.max(t.time.toLong *1000)
    t.time.toLong
  }
}

