package cloud.maxzhao.e_processFunction

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 *检测功能，一定时间内连续上升
 * @author Max
 * @date 2021/4/9 19:23
 */
object processFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new DataSource)

    val processedStream = dataStream.keyBy(_.info)
      .process(new ValueIncreAlert)

    processedStream.print().setParallelism(1)
    env.execute()
  }
}
class ValueIncreAlert extends KeyedProcessFunction[String, DataReading, String]{
  //定义状态,保存上一个状态
  lazy val lastValue = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastValue",classOf[Double]))
  //定义状态,保存定时器状态
  lazy val lastTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTimer",classOf[Long]))

  override def processElement(i: DataReading,
                              context: KeyedProcessFunction[String, DataReading, String]#Context,
                              collector: Collector[String]): Unit = {
    //先取出上一个值
    val preValue = lastValue.value()
    //更新数据
    lastValue.update(i.value)
    //如果温度上升且没有定时器
    if(i.value > preValue && Option(lastTimer.value()).isEmpty){
      //现在时间
      val timestamp = context.timerService().currentProcessingTime()
      //开启5s定时器
      context.timerService().registerProcessingTimeTimer(timestamp + 5000L)
      //保存定时器状态
      lastTimer.update(timestamp + 5000L)
    }else if( i.value < preValue || Option(lastValue.value()).isEmpty) {
      //如果温度下降或者第一次读取，删除定时器，并清空状态
      context.timerService().deleteProcessingTimeTimer(lastTimer.value())
      lastTimer.clear()
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, DataReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey+"温度连续上升！")
    lastTimer.clear()

  }

}
