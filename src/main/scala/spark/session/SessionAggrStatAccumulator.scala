package spark.session

import com.qf.sessionanalyze_gp15.constant.Constants
import com.qf.sessionanalyze_gp15.util.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
  * 使用自定义的数据格式，比如现在要用String格式，可以自定义model
  * 自定义类必须要可序列化，可以基于这种特殊的格式，实现复杂的分布式累加计算
  * 每个task，被分布在集群中各个节点的Executor中运行，可以根据需求，task给Accumulator传入不同的值
  * 最后根据不同的值，做复杂的计算逻辑
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, String] {

  // 设置初始值
  var result =
      Constants.SESSION_COUNT + "=0|" +
      Constants.TIME_PERIOD_1s_3s + "=0|" +
      Constants.TIME_PERIOD_4s_6s + "=0|" +
      Constants.TIME_PERIOD_7s_9s + "=0|" +
      Constants.TIME_PERIOD_10s_30s + "=0|" +
      Constants.TIME_PERIOD_30s_60s + "=0|" +
      Constants.TIME_PERIOD_1m_3m + "=0|" +
      Constants.TIME_PERIOD_3m_10m + "=0|" +
      Constants.TIME_PERIOD_10m_30m + "=0|" +
      Constants.TIME_PERIOD_30m + "=0|" +
      Constants.STEP_PERIOD_1_3 + "=0|" +
      Constants.STEP_PERIOD_4_6 + "=0|" +
      Constants.STEP_PERIOD_7_9 + "=0|" +
      Constants.STEP_PERIOD_10_30 + "=0|" +
      Constants.STEP_PERIOD_30_60 + "=0|" +
      Constants.STEP_PERIOD_60 + "=0|"

  // 判断初始值是否为空
  override def isZero: Boolean = {
    true
  }

  // copy一个新的累加器
  override def copy(): AccumulatorV2[String, String] = {
    val myCopyAccumulator = new SessionAggrStatAccumulator()
    myCopyAccumulator.result = this.result
    myCopyAccumulator
  }

  // 重置一个累加器，将累加器中的数据初始化
  override def reset(): Unit = {
    this.result =
        Constants.SESSION_COUNT + "=0|" +
        Constants.TIME_PERIOD_1s_3s + "=0|" +
        Constants.TIME_PERIOD_4s_6s + "=0|" +
        Constants.TIME_PERIOD_7s_9s + "=0|" +
        Constants.TIME_PERIOD_10s_30s + "=0|" +
        Constants.TIME_PERIOD_30s_60s + "=0|" +
        Constants.TIME_PERIOD_1m_3m + "=0|" +
        Constants.TIME_PERIOD_3m_10m + "=0|" +
        Constants.TIME_PERIOD_10m_30m + "=0|" +
        Constants.TIME_PERIOD_30m + "=0|" +
        Constants.STEP_PERIOD_1_3 + "=0|" +
        Constants.STEP_PERIOD_4_6 + "=0|" +
        Constants.STEP_PERIOD_7_9 + "=0|" +
        Constants.STEP_PERIOD_10_30 + "=0|" +
        Constants.STEP_PERIOD_30_60 + "=0|" +
        Constants.STEP_PERIOD_60 + "=0|"
  }

  // 局部累加，给定具体累加的过程，属于每一个分区进行累加的方法
  override def add(v: String): Unit = {
    // v1是指上次聚合后的值
    val v1 = result
    // 这次传进来的字段名称
    val v2 = v
    //    log.warn("v1 : " + v1 + " v2 : " + v2)
    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      var newResult = ""
      // 从v1中，提取v2对应的值，并累加
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        // 将范围区间原有的值进行累加
        val newValue = oldValue.toInt + 1
        newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
      result = newResult
    }
  }

  // 全局累加，合并每一个分区的累加值
  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case map: SessionAggrStatAccumulator =>
      result = other.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  // 输出值
  override def value: String = result
}
