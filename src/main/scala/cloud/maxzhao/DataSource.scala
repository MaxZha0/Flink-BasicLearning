package cloud.maxzhao

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 *自定义数据source
 * @author Max
 * @date 2021/4/9 16:34
 */
class DataSource() extends SourceFunction[DataReading] {
  //定义一个标志位，表示运行状态
  var running : Boolean = true
  //正常生成
  override def run(sourceContext: SourceFunction.SourceContext[DataReading]): Unit = {
    //初始化一个随机数发生器
    val rand = new Random()
    //初始化定义
    var curValue = 1.to(10).map(
      i => ( "测试数据"+i ,"volt",36 + rand.nextGaussian())
    )
    //产生数据流
    while(running){
      //更新数据值
      curValue = curValue.map(
        t => (t._1, t._2, t._3 + rand.nextGaussian())
      )
      //包装时间值
      val curTime = System.currentTimeMillis()
      curValue.foreach(
        t => sourceContext.collect(DataReading(curTime.toString ,t._1, t._2, t._3))
      )
      Thread.sleep(2000)
    }
  }
  //停止生成
  override def cancel(): Unit = {
    running = false
  }
}
