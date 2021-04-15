package cloud.maxzhao.c_Transform

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 *分流和两种合流方式
 * @author Max
 * @date 2021/4/8 23:31
 */
object b_多流转换算子 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val DataReadingStream = env.addSource(new DataSource)


    //分流算子。目前被弃用，用侧输出流替代
    //Seq()用于分出不同戳的流，目前仅是分区
    val SplitStream = DataReadingStream.split(data =>
      if(data.value > 4) Seq("big") else Seq("small")
    )
    //只有选择出来以后才算是真正分流
    val BigStream = SplitStream.select("big")
    val SmallStream = SplitStream.select("small")
    val AllStream = SplitStream.select("big","small")

//    BigStream.print("big").setParallelism(1)
//    SmallStream.print("small").setParallelism(1)
//    AllStream.print("all").setParallelism(1)



    //两种合并方式 1、connect合并，可以合并不同类型流，联合处理
    val warningStream = BigStream.map( data => (data.info , data.value))
    val connectedStream = warningStream.connect(SmallStream)
    //连合处理，不同无所谓
    val coMapStream = connectedStream.map(
      warningData =>(warningData._1, warningData._2, "warning"),
      smallData =>(smallData.value)
    )
//    coMapStream.print("coMap")



    //两种合并方式 2、union合并，可以合并同类型流，多个合流
    val unionStream = AllStream.union(BigStream).union(SmallStream)
    unionStream.print("union")




    env.execute()

  }

}
