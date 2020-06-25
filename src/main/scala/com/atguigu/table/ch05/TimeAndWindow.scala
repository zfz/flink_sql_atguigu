package com.atguigu.table.ch05

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 1. 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt")
    // val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 3. 生成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      // 提取时间戳，设置Watermark
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 4.1 将流转换成表，定义时间字段：Process Time
    // val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)
    // 4.2 将流转换成表，定义时间字段：Event Time，把timestamp转换成Event Time，直接调用rowtime
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 5. Table API
    // 5.1 Group Window聚合操作
    val aggTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.end)

    // sensorTable.printSchema()
    // 窗口关闭的时候生成聚合结构，不需要用RetractStream
    aggTable.toAppendStream[Row].print("agg result")

    // 5.2 Over Window聚合操作
    val overTable: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      // count & avg 后面必须跟over，否则调用聚合函数
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)
    overTable.toAppendStream[Row].print("over result")

    // 6. SQL操作
    // 6.1 Group Window SQL聚合操作
    tableEnv.createTemporaryView("sensor", sensorTable)
    val aggSQLTable: Table = tableEnv.sqlQuery(
      """
        | select
        |   id
        |   ,count(id), hop_end(ts, interval '4' second, interval '10' second)
        | from sensor
        | group by
        |   id
        |   ,hop(ts, interval '4' second, interval '10' second)
      """.stripMargin)
    aggSQLTable.toAppendStream[Row].print("agg sql")

    // 6.2 Over Window SQL聚合操作
    val overSQLTable: Table = tableEnv.sqlQuery(
      """
        | select
        |   id
        |   ,count(id) over ow
        |   ,avg(temperature) over ow
        | from sensor
        | window ow as (
        |   partition by id
        |   order by ts
        |   rows between 2 preceding and current row
        | )
      """.stripMargin)
    overSQLTable.toAppendStream[Row].print("over sql")

    env.execute("Time and Window")
  }
}
