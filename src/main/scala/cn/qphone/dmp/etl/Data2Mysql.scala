package cn.qphone.dmp.etl

import java.util.Properties

import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.{CommonUtils, SparkUtils}
import org.apache.spark.sql.SaveMode

object Data2Mysql extends LoggerTrait{
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
        val spark = SparkUtils.getLocalSparkSession(Data2Mysql.getClass.getSimpleName)
        spark.sqlContext.setConf(properties)

        //3. 获取数据
        val df = spark.read.parquet(input)

        //4. 注册视图
        df.createOrReplaceTempView("log")

        //5. sql
        val ret = spark.sql(
            """
              |select
              |count(*) ct,
              |provincename,
              |cityname
              |from
              |`log`
              |group by provincename, cityname
              |""".stripMargin)

        //6.输出
        val jdbc = CommonUtils.toMap("db.properties")
        properties.setProperty("user", jdbc("mysql.username"))
        properties.setProperty("password", jdbc("mysql.password"))
        ret.write.mode(SaveMode.Append).jdbc(jdbc("mysql.url"), "dmp", properties)
        //7. 释放
        SparkUtils.stop(spark)
    }
}
