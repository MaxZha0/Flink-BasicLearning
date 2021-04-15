package cloud.maxzhao.e_processFunction

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * 侧输出流
 *
 * @author Max
 * @date 2021/4/9 19:23
 */
object SideOutPut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new DataSource)

    val processedStream = dataStream
      .process(new ValueAlert)

    //取得侧输出
    val sideStream = processedStream.getSideOutput(new OutputTag[String]("Freezing Alert!"))
    sideStream.print()

    env.execute()
  }
}
class ValueAlert extends ProcessFunction[DataReading,DataReading]{
  lazy val alertOutput = new OutputTag[String]("Freezing Alert!")

  override def processElement(i: DataReading,
                              context: ProcessFunction[DataReading, DataReading]#Context,
                              out: Collector[DataReading]): Unit = {
    if( i.value < 34.0){
      //侧输出
      context.output(alertOutput, "Freezing Alert from" + i.info)
    }else{
      out.collect(i)
    }

  }
}
