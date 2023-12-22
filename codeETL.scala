val df = spark.read.option("header", true).csv("MotorVehicle.csv")

import org.apache.spark.sql.types.IntegerType

val df2 = df.withColumn("NUMBER OF PERSONS INJURED", col("NUMBER OF PERSONS INJURED").cast(IntegerType))

val df3 = df2.withColumn("CRASH MONTH", substring(col("CRASH DATE"), 0, 2))

val df4 = df3.withColumn("CRASH YEAR", substring(col("CRASH DATE"), 7, 10))

val df5 = df4.withColumn("YYYYMM", concat(col("CRASH YEAR"), col("CRASH MONTH"))).drop("CRASH MONTH").drop("CRASH YEAR")

val df6 = df5.withColumn("NUMBER OF PERSONS KILLED", col("NUMBER OF PERSONS KILLED").cast(IntegerType))

//new dataframe with columns "YYYYMM", "NUMBER OF PERSONS INJURED", "NUMBER OF PERSONS KILLED"
val df7 = df6.drop("CRASH DATE").drop("CRASH TIME").drop("BOROUGH").drop("ZIP CODE").drop("LATITUDE").drop("LONGITUDE").drop("LOCATION").drop("ON STREET NAME").drop("CROSS STREET NAME").drop("OFF STREET NAME").drop("NUMBER OF PEDESTRIANS INJURED").drop("NUMBER OF PEDESTRIANS KILLED").drop("NUMBER OF CYCLIST INJURED").drop("NUMBER OF CYCLIST KILLED").drop("NUMBER OF MOTORIST INJURED").drop("NUMBER OF MOTORIST KILLED").drop("CONTRIBUTING FACTOR VEHICLE 1").drop("CONTRIBUTING FACTOR VEHICLE 2").drop("CONTRIBUTING FACTOR VEHICLE 3").drop("CONTRIBUTING FACTOR VEHICLE 4").drop("CONTRIBUTING FACTOR VEHICLE 5").drop("COLLISION_ID").drop("VEHICLE TYPE CODE 1").drop("VEHICLE TYPE CODE 2").drop("VEHICLE TYPE CODE 3").drop("VEHICLE TYPE CODE 4").drop("VEHICLE TYPE CODE 5")

df7.coalesce(1).write.option("header",true).csv("FirstText")
