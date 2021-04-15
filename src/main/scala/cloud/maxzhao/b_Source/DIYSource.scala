package cloud.maxzhao.b_Source

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 *
 * @author Max
 * @date 2021/4/9 0:01
 */
object DIYSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new DataSource)

    dataStream.print()
    env.execute()

  }

}

