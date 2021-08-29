from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
rawDF = spark.read.json("ipl_json", multiLine = "true")
innings=rawDF.select(explode("innings").alias("innings"))
rawDF1=innings.select(col("innings.team"),explode("innings.overs.deliveries").alias("batbowl"))
rawDF2=rawDF1.select(col("batbowl.batter").alias("batter"),col("batbowl.bowler").alias("bowler"))
rawDF3=rawDF2.select(col("batter"),col("bowler"))
rawDF4=rawDF3.withColumn("batter",arrays_zip("batter")).select("bowler",explode("batter").alias("merge")).select("bowler",col("merge.batter"))
total_runs=innings.withColumn("deliveries",explode("innings.overs.deliveries")).withColumn("runs",explode("deliveries.runs")).select("runs.total")
d1=rawDF4.withColumn("id",monotonically_increasing_id())
d2=total_runs.withColumn("id",monotonically_increasing_id())
d3=d2.join(d1,"id","outer").drop("id")
d4=d3.groupBy("batter","bowler").agg(sum("total"))
d5=d4.orderBy("batter",decending=False)
rawDF5=d5.withColumn("result",array_distinct("bowler"))
rawDF5.show(50,False)

