package cn.qphone.dmp.app

import java.util.Properties

import cn.qphone.dmp.etl.{Data2Json, Log2Parquet}
import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.{RtbUtils, SparkUtils}
import org.apache.spark.rdd.RDD

/**
 * 地域分布指标
 */
object LocationRptV2 extends LoggerTrait{
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
        val spark = SparkUtils.getLocalSparkSession(LocationRptV2.getClass.getSimpleName)
        spark.sqlContext.setConf(properties)

        //3. 获取数据
        val df = spark.read.parquet(input)

        //4. 获取对应字段
        val words = df.rdd.map(row => {
            //4.1 获取需要的字段
            val requestmode:Int = row.getAs[Int]("requestmode")
            val processnode = row.getAs[Int]("processnode")
            val iseffective = row.getAs[Int]("iseffective")
            val isbilling = row.getAs[Int]("isbilling")
            val isbid = row.getAs[Int]("isbid")
            val iswin = row.getAs[Int]("iswin")
            val adorderid = row.getAs[Int]("iswin")
            val winprice = row.getAs[Double]("winprice")
            val adpayment = row.getAs[Double]("adpayment")

            //4.2 处理业务
            val reqList:List[Double] = RtbUtils.requestAd(requestmode, processnode) // 告诉我你到底是一个什么请求
            val adList:List[Double] = RtbUtils.adPrice(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment) // 竞价和广告
            val clickList: List[Double] = RtbUtils.shows(requestmode, iseffective) // 展示和点击

            //4.3 获取省市
            val pro = row.getAs[String]("provincename")
            val city = row.getAs[String]("cityname")
            ((pro, city), reqList ++ adList ++ clickList)
        })

        //5. 聚合
        words.reduceByKey((list1, list2) => {
            list1.zip(list2).map(t => t._1 + t._2)
        }).foreach(println)


        //6. 释放资源
        SparkUtils.stop(spark)
    }
}
