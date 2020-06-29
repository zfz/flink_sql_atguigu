package com.atguigu.table.ch06_udf

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * 1: 1
 */
object ScalarFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt")
    // val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 2. 转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      // 分配时间戳和Watermark
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 3. 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 4. 使用自定义的Hash函数，求ID的Hash值
    val hashCode = new HashCode(1.23)

    // 5.1 Table API 调用方式
    val resultTable: Table = sensorTable
      .select('id, 'ts, hashCode('id))
    resultTable.toAppendStream[Row].print("table")

    // 5.2 SQL调用方式，首先要注册表和函数
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSQLTable: Table = tableEnv.sqlQuery(
      """
        |select id, ts, hashCode(id)
        |from sensor
      """.stripMargin)
    resultSQLTable.toAppendStream[Row].print("sql")

    env.execute("Scalar UDF")
  }
}

/**
 * 自定义求HashCode的标量函数
 * 1行数据输入，结果1行
 */
class HashCode(factor: Double) extends ScalarFunction {
  // 必须实现eval方法，但是也没写在ScalarFunction的抽象类里
  // 返回值不能为Unit
  def eval( value: String ): Int = {
    (value.hashCode * factor).toInt
  }
}