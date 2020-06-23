package cn.qphone.dmp.app

import java.util.Properties

import cn.qphone.dmp.etl.Data2Json.SPARK_PROPERTIES
import cn.qphone.dmp.etl.{Data2Json, Log2Parquet}
import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.SparkUtils

/**
 * 地域分布指标
 */
object LocationRpt extends LoggerTrait{
    val SPARK_PROPERTIES:String = "spark.properties"
    def main(args: Array[String]): Unit = {
        //1. 控制读取和存储的参数
        if(args == null || args.length != 2) {
            println("Usage : <input> <output>")
            System.exit(-1)
        }
        val Array(input, output) = args
        //2. 获取到入口并配置序列化以及压缩方式
        val properties = new Properties()
        properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))
        val spark = SparkUtils.getLocalSparkSession(LocationRpt.getClass.getSimpleName)
        spark.sqlContext.setConf(properties)

        //3. 获取数据
        val df = spark.read.parquet(input)

        //4. 注册视图
        df.createOrReplaceTempView("log")

        //5. sql
        val ret = spark.sql(
            """
              |select provincename, cityname,
              |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ys_request_cnt,
              |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yx_request_cnt,
              |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) ad_request_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cy_bid_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bid_succ_cnt,
              |sum(case when iseffective = 1 and requestmode = 2 then 1 else 0 end) show_cnt,
              |sum(case when iseffective = 1 and requestmode = 3 then 1 else 0 end) click_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 1000 else 0.0 end) price_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 1000 else 0.0 end) ad_cnt
              |from log
              |group by provincename, cityname
              |""".stripMargin)

        //6. 数据存储
        ret.write.partitionBy("provincename", "cityname").save(output)

        //7. 释放资源
        SparkUtils.stop(spark)
    }
}
