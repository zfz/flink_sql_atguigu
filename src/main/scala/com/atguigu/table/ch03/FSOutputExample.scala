package com.atguigu.table.ch03

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FSOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 读取数据转换成流，Map成样例类
    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 3. 把流转换成表
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp as 'ts)

    // 4. 进行表的转换操作
    // 4.1 简单查询转换
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")
    // 4.2 聚合转换
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    // 5. 将结果表输出到文件中
    tableEnv.connect(new FileSystem().path("src/main/resources/output.log"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("outputTable")

    resultTable.insertInto("outputTable")
    // 聚合结果不能写入文件
    // aggResultTable.insertInto("outputTable")

    env.execute("FS Output Example")
  }
}
