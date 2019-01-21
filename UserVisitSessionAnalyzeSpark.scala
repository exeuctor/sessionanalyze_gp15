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
  /**
    * 计算出各个范围的session占比，并存入数据库
    *
    * @param value
    * @param taskid
    */
  private def calcuateAndPersistAggrStat(value: String, taskid: Long): Unit = {

    // 首先从Accumulator统计的字符串中获取各个聚合的值
    val session_count = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT)).toLong
    val visit_length_1s_3s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s)).toLong
    val visit_length_4s_6s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s)).toLong
    val visit_length_7s_9s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s)).toLong
    val visit_length_10s_30s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s)).toLong
    val visit_length_30s_60s = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s)).toLong
    val visit_length_1m_3m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m)).toLong
    val visit_length_3m_10m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m)).toLong
    val visit_length_10m_30m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m)).toLong
    val visit_length_30m = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m)).toLong
    val step_length_1_3 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3)).toLong
    val step_length_4_6 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6)).toLong
    val step_length_7_9 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9)).toLong
    val step_length_10_30 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30)).toLong
    val step_length_30_60 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60)).toLong
    val step_length_60 = (StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60)).toLong

    // 计算访问时长和访问步长的范围占比
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)
    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)

    // 将统计结果存入数据库
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }

  /**
    * 获取通过筛选条件的Session的访问明细
    *
    * @param filteredSessionId2AggrInfoRDD
    * @param sessionId2ActionRDD
    * @return
    */
  private def getSessionId2DetailRDD(filteredSessionId2AggrInfoRDD: RDD[(String, String)], sessionId2ActionRDD: RDD[(String, Row)]) = {
    // 过滤后的数据和访问明细数据进行join
    val sessionId2DetailRDDTmp = filteredSessionId2AggrInfoRDD.join(sessionId2ActionRDD)
    // 整合数据
    val sessionId2DetailRDD = sessionId2DetailRDDTmp.map(tup => (tup._1, tup._2._2))

    sessionId2DetailRDDTmp
  }

  /**
    * 按照使用者提供的条件过滤session数据并进行聚合
    *
    * @param sessionId2AggInfoRDD       基础数据
    * @param taskParam                  使用者的条件
    * @param sessionAggrStatAccumulator 累加器
    * @return
    */
  private def filteredSessionAndAggrStat(sessionId2AggInfoRDD: RDD[(String, String)], taskParam: JSONObject, sessionAggrStatAccumulator: SessionAggrStatAccumulator) = { // 首先把所有的使用者的筛选参数取出来并拼接
    // 获取使用者提交的条件
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categorys = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 拼接
    var _paramter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|"
    else "") + (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|"
    else "") + (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|"
    else "") + (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|"
    else "") + (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|"
    else "") + (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|"
    else "") + (if (categorys != null) Constants.PARAM_CATEGORY_IDS + "=" + categorys + "|"
    else "")

    // 把_parameter的值的末尾的"|"截取掉
    if (_paramter.endsWith("|")) _paramter = _paramter.substring(0, _paramter.length - 1)
    val parameter = _paramter

    // 初始化点击时长和步长
    var visitLength = 0L
    var stepLength = 0L

    // 根据筛选条件进行过滤以及聚合
    val filteredSessionAggrInfoRDD = sessionId2AggInfoRDD.filter(filterCondition(_, parameter, sessionAggrStatAccumulator))

    filteredSessionAggrInfoRDD
  }

  /**
    * 按照使用者条件进行过滤
    * @param tuple
    * @param parameter
    * @param sessionAggrStatAccumulator
    * @return
    */
  private def filterCondition(tuple: (String, String), parameter: String, sessionAggrStatAccumulator: SessionAggrStatAccumulator): Boolean = {

    // 从tup中获取基础数据
    val aggrInfo = tuple._2

    /**
      * 依次按照筛选条件进行过滤
      */
    // 按照年龄进行过滤
    if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) return false
    // 按照职业进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) return false
    // 按照城市进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) return false
    // 按照性别进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) return false
    // 按照关键字进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) return false
    // 按照点击品类进行过滤
    if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) return false

    /**
      * 执行到这里，说明该Session通过了用户指定的筛选条件
      * 接下来要对Session的访问时长和访问步长进行统计
      */
    // 首先累加session count
    sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

    // 根据Session对应的时长和步长的时间范围进行累加操作
    val visitLength = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)).toLong
    val stepLength = (StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH)).toLong

    // 计算访问时长范围
    if (visitLength >= 1 && visitLength <= 3) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength < 30) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength >= 30 && visitLength < 60) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength >= 60 && visitLength < 180) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength >= 180 && visitLength < 600) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength >= 600 && visitLength < 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength >= 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)

    // 计算访问步长范围
    if (stepLength >= 1 && stepLength <= 3) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength < 30) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength >= 30 && stepLength < 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength >= 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)

    true
  }

  /**
    * 对行为数据按照session粒度进行聚合
    *
    * @return
    */
  private def aggregateBySession(sc: JavaSparkContext, spark: SparkSession, sessionId2ActionRDD: RDD[(String, Row)]) = {
    // 首先对行为数据进行分组
    val sessionId2ActionPairRDD = sessionId2ActionRDD.groupByKey

    // 对每个session分组进行聚合，将session中所有的搜索关键字和点击品类都聚合起来
    // 格式：<userId, partAggrInfo(sessionId, searchKeywords, clickCategoryIds, visitLength, stepLength, startTime)>
    val userId2PartAggrInfoRDD = sessionId2ActionPairRDD.map(tup => {
      val sessionId = tup._1
      val it = tup._2.iterator

      // 用来存储搜索关键字和点击品类
      val searchKeywordsBuffer = new StringBuffer
      val clickCategoryIdsBuffer = new StringBuffer

      // 用来存储userId
      var userId = 0L

      // 用来存储起始时间和结束时间
      var startTime = new Date()
      var endTime = new Date()

      // 用来存储session的访问步长
      var stepLength = 0

      // 遍历session中的所有访问行为
      while (it.hasNext) {
        // 提取每个访问行为的搜索关键字和点击品类
        val row = it.next

        userId = row.getLong(1)

        // 注意：如果该行为数据属于搜索行为，searchKeyword是有值的
        //  如果该行为数据是点击品类行为，clickCategoryId是有值的
        //  但是，任何的一个行为，不可能两个字段都有值
        val searchKeyword = row.getString(5)
        val clickCategoryId = String.valueOf(row.getAs[Long](6))

        // 追加搜索关键字
        if (!StringUtils.isEmpty(searchKeyword)) if (!searchKeywordsBuffer.toString.contains(searchKeyword)) searchKeywordsBuffer.append(searchKeyword + ",")

        // 追加点击品类
        if (clickCategoryId != null) if (!clickCategoryIdsBuffer.toString.contains(clickCategoryId)) clickCategoryIdsBuffer.append(clickCategoryId + ",")

        // 计算session的开始时间和结束时间
        val actionTime = DateUtils.parseTime(row.getString(4))
        if (actionTime.before(startTime)) startTime = actionTime
        if (actionTime.after(endTime)) endTime = actionTime

        // 计算访问步长
        stepLength += 1
      }

      // 截取搜索关键字和点击品类的两端的","
      val searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
      val clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      // 计算访问时长，单位秒
      val visitLength = (endTime.getTime - startTime.getTime) / 1000

      // 聚合数据，数据以字符串拼接的方式：key=value|key=value|key=value
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELD_SEARCH_KEYWORDS +
        "=" + searchKeywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"

      (userId, partAggrInfo)
    })

    // 查询所有的用户数据，映射成<userId, Row>
    val sql = "select * from user_info"
    val userInfoRDD = spark.sql(sql).rdd
    val userId2InfoRDD = userInfoRDD.map(userrow => (userrow.getLong(0), userrow))

    // 将session粒度的聚合数据（userId2PartAggrInfoRDD）和用户信息进行join，
    // 生成的格式为：<userId, <sessionInfo, userInfo>>
    val userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD)

    // 对join后的数据进行拼接，并返回<sessionId, fullAggrInfo>格式的数据
    val sessionId2FullAggrInfoRDD = userId2FullInfoRDD.map(tuple => {
      val partAggrInfo = tuple._2._1
      // 获取用户信息
      val userInfoRow = tuple._2._2
      // 获取sessionId
      val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      // 获取用户信息的age
      val age = userInfoRow.getInt(3)
      // 获取用户信息的职业
      val professional = userInfoRow.getString(4)
      // 获取用户信息的所在城市
      val city = userInfoRow.getString(5)
      // 获取用户信息的性别
      val sex = userInfoRow.getString(6)
      // 拼接
      val fullAggrInfo = partAggrInfo + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex + "|"

      (sessionId, fullAggrInfo)
    })

    sessionId2FullAggrInfoRDD
  }


}