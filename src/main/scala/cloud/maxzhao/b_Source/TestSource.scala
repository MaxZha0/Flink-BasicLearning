package cloud.maxzhao.b_Source

import cloud.maxzhao.DataReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}



/**
 *
 * @author Max
 * @date 2021/4/8 22:43
 */
object TestSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1、从自定义的集合中读取数据
    val stream1 = env.fromCollection(List (
      DataReading( "2021-4-8", "测试数据", "volt", 32.6 ),
      DataReading( "2021-4-8", "测试数据", "volt", 31.6 ),
      DataReading( "2021-4-8", "测试数据", "volt", 33.6 ),
      DataReading( "2021-4-8", "测试数据", "volt", 34.6 )
    ))
    stream1.print().setParallelism(1)

    //2、从文件中读取数据
    val stream2 = env.readTextFile("src/main/resources/dataReading.txt")
    stream2.print().setParallelism(1)

    //3、从元素中读取数据
    val stream3 = env.fromElements(1,2,3,"asd",DataReading("2000-1-1","a","a",36))
    stream3.print().setParallelism(1)




    env.execute("source test")
  }
}
