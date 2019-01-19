package spark.session

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.utils.SparkUtils
import com.qf.sessionanalyze_gp15.constant.Constants
import com.qf.sessionanalyze_gp15.dao.factory.DAOFactory
import com.qf.sessionanalyze_gp15.domain._
import com.qf.sessionanalyze_gp15.util.DateUtils
import com.qf.sessionanalyze_gp15.util.NumberUtils
import com.qf.sessionanalyze_gp15.util.ParamUtils
import com.qf.sessionanalyze_gp15.util.StringUtils
import com.qf.sessionanalyze_gp15.util.ValidUtils
import org.apache.spark.api.java.JavaSparkContext
import java.util

import scala.collection.JavaConversions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 获取用户访问Session数据进行分析
  * 1、获取使用者创建的任务信息
  * 任务信息中的过滤条件有：
  * 时间范围：起始日期-结束日期
  * 年龄范围
  * 性别
  * 所在城市
  * 用户搜索的关键字
  * 点击品类
  * 点击商品
  * 2、Spark作业是如何接收使用者创建的任务信息
  * shell脚本通知--SparkSubmit
  * 从MySQL的task表中根据taskId来获取任务信息
  * 3、Spark作业开始数据分析
  */
object UserVisitSessionAnalyzeSpark {
  def main(args: Array[String]): Unit = {
    /* 模板代码 */
    // 创建配置信息对象
    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
    SparkUtils.setMaster(conf)

    // 上下文对象，集群的入口类
    val sc = new SparkContext(conf)

    // SparkSQL的上下文
    val spark = SparkUtils.getSparkSession()

    // 设置检查点
    // sc.checkpointFile("hdfs://node01:9000.....");

    // 生成模拟数据
    SparkUtils.mockData(sc, spark)

    // 创建获取任务信息的实例
    val taskDAO = DAOFactory.getTaskDAO

    // 获取指定的任务，获取到taskId
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)

    val task = taskDAO.findById(taskId)

    if (task == null) {
      System.out.println(new Date + "亲，你没有获取到taskId对应的task信息")
    }

    // 获取taskId对应的任务信息，也就是task_param字段对应的值
    // task_param的值就使用者提供的查询条件
    val taskParam = JSON.parseObject(task.getTaskParam)

    // 查询指定日期范围内的行为数据（点击、搜索、下单 、支付）
    // 首先要从user_visit_action这张hive表中查询出按照指定日期范围得到的行为数据
    val actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParam)

    // 生成Session粒度的基础数据，得到的数据格式为：<sessionId, actionRDD>
    //import spark.implicits._
    val sessionId2ActionRDD = actionRDD.map(row => (row(2).toString, row))

    // 对于以后经常用到的基础数据，最好缓存起来，便于以后快速的获取
    val sessionId2ActionRDDCache = sessionId2ActionRDD.cache();



    spark.stop()
  }



}