package cloud.maxzhao.f_state

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * 状态后端
 * @author Max
 * @date 2021/4/9 19:23
 */
object 检测跳变 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new DataSource)

//    //打开检查点储存，（周期），一般不用
//    env.enableCheckpointing(10000)
//    //使用内存状态后端
//    env.setStateBackend(new MemoryStateBackend())
//    //使用文件路径状态后端
//    env.setStateBackend(new FsStateBackend("psth")
//    //使用rocksDB状态后端，需要第三方程序引入
//    env.setStateBackend(new RocksDB())

    val thresholdValue : Double = 5.0
    val processedStream = dataStream.keyBy(_.info)
      .process(new ValueChangeAlert(thresholdValue))

    processedStream.print().setParallelism(1)
    env.execute()
  }
}
class ValueChangeAlert(thresholdValue : Double) extends KeyedProcessFunction[String, DataReading, String]{
  //定义状态,保存上一个状态
  lazy val maxValueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("maxValue",classOf[Double]))
  lazy val minValueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("minValue",classOf[Double]))


  override def processElement(i: DataReading,
                              context: KeyedProcessFunction[String, DataReading, String]#Context,
                              out: Collector[String]): Unit = {
    //给最小记录值一个初始大数
    if(Option(minValueState.value()).isEmpty){
      minValueState.update(40)
    }
    //跟新最大和最小值记录
    if(i.value > maxValueState.value()){
      maxValueState.update(i.value)
    }else if(i.value < minValueState.value()){
      minValueState.update(i.value)
    }

    if((maxValueState.value() - minValueState.value()) > thresholdValue){
      out.collect(i.info+ " 波峰波谷差值过大！")
      maxValueState.clear()
      minValueState.clear()
    }
  }



}
