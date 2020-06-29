package com.atguigu.table.ch06_udf

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * N: M
 */
object TableAggFunctionExample {
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

    // 4. 创建一个表聚合函数的实例
    val top2Temperature = new Top2Temperature()

    // 5. Table API调用
    val resultTable: Table = sensorTable
      .groupBy('id)
      .flatAggregate(top2Temperature('temperature) as ('temperature, 'rank))
      .select('id, 'temperature, 'rank)

    resultTable.toRetractStream[Row].print("result")

    env.execute("UDTAF")
  }
}

/**
 * 自定义状态类
 */
class Top2TemperatureAcc {
  var first: Double = Double.MinValue
  var second: Double = Double.MinValue
}

/**
 * 自定义表聚合函数，实现Top2功能，输出（temperature，rank）
 * T 返回类型
 * ACC 聚合类型
 * 输入类型在eval中指定
 * N行数据输入，输出M行
 */
class Top2Temperature() extends TableAggregateFunction[(Double, Int), Top2TemperatureAcc] {
  // 初始化状态
  override def createAccumulator(): Top2TemperatureAcc = new Top2TemperatureAcc()

  // 每来一个数据后，聚合计算的操作
  def accumulate(acc: Top2TemperatureAcc, temperature: Double): Unit = {
    // 将当前温度值，跟状态中的最高温和第二高温比较，如果大的话就替换
    if (temperature > acc.first) {
      acc.second = acc.first
      acc.first = temperature
    } else if (temperature > acc.second) {
      acc.second = temperature
    }
  }

  // 实现一个输出数据的方法，写入结果表中
  def emitValue(acc: Top2TemperatureAcc, out: Collector[(Double, Int)]): Unit = {
    out.collect(acc.first, 1)
    out.collect(acc.second, 2)
  }
}