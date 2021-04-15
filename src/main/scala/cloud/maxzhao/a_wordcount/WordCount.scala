package cloud.maxzhao.a_wordcount

import org.apache.flink.api.scala._

/**
 *批处理wordcount程序
 *
 * @author Max
 * @date 2021/4/8 14:46
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境(批处理)
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件获取
    val inputPath = "src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    //切分数据，得到word,然后做分组聚合
    val wordCountDataSet = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)


    wordCountDataSet.print()
  }
}
