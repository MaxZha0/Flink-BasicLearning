package cloud.maxzhao.g_容错

import cloud.maxzhao.{DataReading, DataSource}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 *
 * @author Max
 * @date 2021/4/9 19:23
 */
object 检测跳变 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.addSource(new DataSource)

    //打开检查点储存，（周期），一般不用
    env.enableCheckpointing(10000)
    //同上
    env.getCheckpointConfig.setCheckpointInterval(10000)
    //检查点策略
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //超时后checkpoint失效
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    //当检查点异常，结束任务，高精确度
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //最大同时进行的检查点数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //两次检查点之间的最小时间间隔，和上述设置冲突
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    //开启检查点的外部持久化，当job失败，检查点会被清理，这样设置会保存下来
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    //重启策略，尝试三次，间隔500ms
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))




    dataStream.print().setParallelism(1)
    env.execute()
  }
}
