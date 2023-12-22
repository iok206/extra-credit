val df = spark.read.option("header", true).csv("MotorVehicle.csv")
df.printSchema()

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val df2 = df.withColumn("NUMBER OF PERSONS INJURED", col("NUMBER OF PERSONS INJURED").cast(IntegerType))
df2.printSchema()

df2.select(avg($"NUMBER OF PERSONS INJURED")).show()

val median = df2.stat.approxQuantile("NUMBER OF PERSONS INJURED", Array(0.5), 0.01)
val median2 = median(0)
println(s"The median is: $median2")

val mode = df2.groupBy("NUMBER OF PERSONS INJURED").count().orderBy(desc("count")).first()
println("Mode", mode.get(0))

val stdDev = stddev("NUMBER OF PERSONS INJURED")
println(stdDev)

df2.count()