package cloud.maxzhao.b_Source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties

/**
 *kafka 连接
 * @author Max
 * @date 2021/4/8 23:31
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaBrokers = "n000:9092,n001:9092,n002:9092,n003:9092"
    val consumerGroup = "readData"
    val dataTopic = "DataGenerationTopic"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokers)
    properties.setProperty("group.id", consumerGroup)
    properties.setProperty("auto.offset.reset", "latest")

    //source
    val DataStream = env.addSource(new FlinkKafkaConsumer011[String](dataTopic, new SimpleStringSchema(), properties))

    DataStream.print().setParallelism(1)

    //sink
    DataStream
      .addSink(new FlinkKafkaProducer011[String](kafkaBrokers, dataTopic, new SimpleStringSchema()))
      .setParallelism(1)

    env.execute()

  }

}
