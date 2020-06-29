package com.atguigu.table.ch06_udf

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * N to 1
 */
object AggregateFunctionExample {
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

    // 4. 创建一个聚合函数的实例
    val avgTemperature = new AvgTemperature()

    // 5. Table API调用
    val resultTable = sensorTable
      .groupBy('id)
      .aggregate(avgTemperature('temperature) as 'avg_temperature )
      .select('id, 'avg_temperature)

    resultTable.toRetractStream[Row].print("avg result")

    env.execute("UDAF")
  }
}


/**
 * 自定义聚合函数
 * T 返回类型
 * ACC 聚合类型
 * 输入类型在eval中指定
 **/
class AvgTemperature extends AggregateFunction[Double, AvgTemperatureACC] {
  override def getValue(acc: AvgTemperatureACC): Double = acc.sum / acc.cnt

  override def createAccumulator(): AvgTemperatureACC = new AvgTemperatureACC()

  def accumulate(acc: AvgTemperatureACC, temperature: Double): Unit = {
    acc.sum += temperature
    acc.cnt += 1
  }
}

/**
 * 专门定义一个聚合函数的状态类，用于保存聚合状态(sum，count)
 * N行数据输入，输出1行
 **/
class AvgTemperatureACC {
  var sum: Double = 0.0
  var cnt: Int = 0
}