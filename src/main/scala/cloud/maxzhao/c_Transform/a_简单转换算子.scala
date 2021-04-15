package cloud.maxzhao.c_Transform

import cloud.maxzhao.DataReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 *
 * @author Max
 * @date 2021/4/8 23:31
 */
object a_简单转换算子 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaBrokers = "n000:9092,n001:9092,n002:9092,n003:9092"
    val consumerGroup = "readData"
    val dataTopic = "DataGenerationTopic"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)
    properties.setProperty("auto.offset.reset", "latest")

    val DataStream = env.addSource(new FlinkKafkaConsumer011[String](dataTopic, new SimpleStringSchema(), properties))



    val DataReadingStream = DataStream
      //map 用于格式转换， .trim用于消除空格
      .map( data => {
      val dataArray = data.split("\t")
      DataReading(dataArray(0), dataArray(1).trim, dataArray(2), dataArray(3).toDouble)
    })
      //filter可以直接用于过滤数据
      .filter(x => x.value < 5)

    val endStream = DataReadingStream
      //keyBy用于分区
      .keyBy(_.ID)
      //每一个分区的简单聚合结果
      .max(3)

    val endStream2 = DataReadingStream
      .keyBy(_.ID)
      //更广泛使用的聚合,只能聚合成原来类型
      .reduce( (x,y) => DataReading(y.time, y.info, y.ID, x.value + y.value) )

    endStream2.print().setParallelism(1)






    env.execute()

  }

}
